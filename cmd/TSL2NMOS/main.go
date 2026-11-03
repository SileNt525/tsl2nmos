package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"tsl2nmos/internal/config"
	"tsl2nmos/internal/controller"
	"tsl2nmos/internal/nmos"
	"tsl2nmos/internal/tally"
	"tsl2nmos/internal/tsl"
	"tsl2nmos/internal/web"
)

var Version = "1.0.0"

func main() {
	var (
		cfgPath     = flag.String("config", "configs/config.yaml", "YAML 配置文件路径（不存在时自动生成默认配置）")
		httpPort    = flag.Int("http-port", 0, "覆盖 HTTP/NMOS/WebSocket/Web 控制台监听端口（默认读取配置文件 8080）")
		tslMode     = flag.String("tsl-mode", "", "覆盖 TSL 接入模式: listen(默认被动UDP+TCP), tcp_client(主动拉TCP Listener), udp_client")
		tslListen   = flag.String("tsl-listen", "", "覆盖 TSL 5.0 被动监听地址，默认 0.0.0.0:8901")
		tslRemote   = flag.String("tsl-remote", "", "当导播台作为 Listener 时主动连接的导播台地址，如 192.168.42.100:8901")
		hostIP      = flag.String("host-ip", "", "覆盖 NMOS 对外宣告的本机网卡 IP（默认 auto 自动路由匹配）")
		logLevelStr = flag.String("log-level", "info", "日志级别: debug, info, warn, error")
		showVersion = flag.Bool("version", false, "打印版本号并退出")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("TSL2NMOS v%s\n", Version)
		return
	}

	logger := initLogger(*logLevelStr)

	cfgMgr, err := config.LoadOrCreate(*cfgPath)
	if err != nil {
		logger.Error("加载或初始化配置文件失败", "path", *cfgPath, "err", err)
		os.Exit(1)
	}

	cfg := cfgMgr.Get()
	cliModified := false
	if *httpPort > 0 && cfg.Node.HTTPPort != *httpPort {
		cfg.Node.HTTPPort = *httpPort
		cliModified = true
	}
	if *tslMode != "" && cfg.TSL.TransportMode != *tslMode {
		cfg.TSL.TransportMode = *tslMode
		cliModified = true
	}
	if *tslListen != "" && cfg.TSL.ListenAddr != *tslListen {
		cfg.TSL.ListenAddr = *tslListen
		cliModified = true
	}
	if *tslRemote != "" && cfg.TSL.RemoteAddr != *tslRemote {
		cfg.TSL.RemoteAddr = *tslRemote
		cliModified = true
	}
	if *hostIP != "" && cfg.Node.HostIP != *hostIP {
		cfg.Node.HostIP = *hostIP
		cliModified = true
	}
	if cliModified {
		_ = cfgMgr.Save(cfg)
		cfg = cfgMgr.Get()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 1. Initialize authoritative NMOS resource store.
	store := nmos.NewStore(cfg)

	// 2. Initialize Tally state & mapping engine.
	engine := tally.NewEngine(cfg, func(channelID int, color tally.Color) (string, string) {
		return nmos.ChannelSourceID(cfg.Node.IDSeed, channelID, color),
			nmos.ChannelSenderID(cfg.Node.IDSeed, channelID, color)
	})

	// Sync initial channel values (important if InvertRed/InvertGreen is set).
	store.RebuildFromConfig(cfg, func(chID int, c tally.Color) bool {
		val, _ := engine.GetChannelValue(chID, c)
		return val
	})

	// 3. Initialize IS-07 WebSocket transport hub.
	wsHub := nmos.NewWSHub(store, logger)

	// Wire real-time Tally transitions -> NMOS Store -> IS-07 WebSocket broadcast!
	engine.OnChange(func(ev tally.StateChangeEvent) {
		msg, masterEnabled, exists := store.UpdateTallyValue(ev.ChannelID, ev.Color, ev.Value, ev.Timestamp)
		if exists && masterEnabled {
			wsHub.BroadcastState(msg)
		}
		logger.Debug("Tally 状态跳变",
			"channel", ev.ChannelID,
			"color", ev.Color,
			"value", ev.Value,
			"master_enable", masterEnabled,
		)
	})

	go engine.StartWatchdog(ctx)

	// 4. Start mDNS responder & scanner and IS-04 Registration client.
	mdnsSvc := nmos.NewMDNSService(store, logger)
	if cfg.NMOS.MDNSEnabled {
		_ = mdnsSvc.Start(ctx)
	}

	regCli := nmos.NewRegistryClient(store, mdnsSvc, cfg.NMOS, logger)
	store.SetOnResourceChange(func() {
		mdnsSvc.AnnounceNow()
		regCli.TriggerSync()
	})
	regCli.Start(ctx)

	// 5. Start Direct IS-05 Receiver Patcher (for Riedel StageLink NSA-010C standalone/hybrid operation).
	patcher := controller.NewDirectPatcher(cfgMgr, store, logger)
	patcher.Start(ctx)

	// 6. Start TSL 5.0 transport listener / active connector.
	tslListener := tsl.NewListener(cfg.TSL.ListenAddr, engine.HandlePacket, logger)
	tslListener.SetTransportMode(cfg.TSL.TransportMode, cfg.TSL.ListenAddr, cfg.TSL.RemoteAddr)
	if err := tslListener.Start(ctx); err != nil {
		logger.Error("启动 TSL 5.0 输入模块失败", "mode", cfg.TSL.TransportMode, "addr", cfg.TSL.ListenAddr, "err", err)
		os.Exit(1)
	}

	// 7. Mount NMOS APIs + Embedded Web Console onto unified HTTP server.
	onReload := func(newCfg config.Config) {
		if err := tslListener.ApplyTransport(newCfg.TSL.TransportMode, newCfg.TSL.ListenAddr, newCfg.TSL.RemoteAddr); err != nil {
			logger.Error("热切换 TSL 输入模式/端口失败", "mode", newCfg.TSL.TransportMode, "err", err)
		}
		engine.ApplyConfig(newCfg)
		store.RebuildFromConfig(newCfg, func(chID int, c tally.Color) bool {
			val, _ := engine.GetChannelValue(chID, c)
			return val
		})
		regCli.UpdateConfig(newCfg.NMOS)
		patcher.TriggerSync()
		logger.Info("配置已完成热重载", "channels", len(newCfg.Channels), "direct_targets", len(newCfg.DirectTargets))
	}

	mux := http.NewServeMux()
	nmos.RegisterRoutes(mux, store, wsHub)
	webSrv := web.NewServer(cfgMgr, tslListener, engine, store, wsHub, regCli, mdnsSvc, patcher, onReload, logger)
	webSrv.RegisterRoutes(mux)

	httpListenAddr := net.JoinHostPort(cfg.Node.BindAddr, fmt.Sprintf("%d", cfg.Node.HTTPPort))
	httpServer := &http.Server{
		Addr:              httpListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	advIP, advPort := store.HostEndpoint()
	logger.Info("TSL2NMOS 网关启动就绪",
		"version", Version,
		"config", cfgMgr.Path(),
		"tsl_udp", tslListener.BoundAddr(),
		"web_ui", fmt.Sprintf("http://%s:%d/", advIP, advPort),
		"nmos_node_api", fmt.Sprintf("http://%s:%d/x-nmos/node/v1.3/", advIP, advPort),
		"nmos_is07_ws", fmt.Sprintf("ws://%s:%d/x-nmos/events/v1.0/ws", advIP, advPort),
	)

	errCh := make(chan error, 1)
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("收到退出信号，正在发送 IS-07 shutdown 通知并优雅关闭服务...")
	case serveErr := <-errCh:
		logger.Error("HTTP 服务异常退出", "err", serveErr)
		os.Exit(1)
	}

	wsHub.Shutdown()

	shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutCtx)
	logger.Info("TSL2NMOS 已安全退出")
}

func initLogger(levelStr string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(strings.TrimSpace(levelStr)) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}
