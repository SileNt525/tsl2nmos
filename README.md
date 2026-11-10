# `TSL2NMOS`

**Lightweight Broadcast-Grade Bidirectional TSL 5.0 (UDP/TCP) ⇄ AMWA NMOS IS-07 Event & Tally Gateway**  
**TSL 5.0 (UDP/TCP) ⇄ AMWA NMOS IS-07 双向轻量级广播级 Tally 网关**

[English](#english) | [中文说明](#chinese-readme)

---

<a name="english"></a>
## English

`TSL2NMOS` is an ultra-lightweight, zero-external-dependency **bidirectional** protocol gateway engineered for modern IP broadcast facilities (SMPTE ST 2110 / AMWA NMOS), featuring a real-time **Bilingual (`中文 | EN`) Web Console**:
- **Forward `TSL → NMOS`**: Converts incoming **TSL UMD v5.0 (UDP / TCP, supporting both passive listening and active client pull)** red/green tally signals into **AMWA NMOS IS-07 (Event & Tally over WebSocket)** boolean event streams with sub-millisecond latency, directly actuating physical relay dry contacts on any NMOS IS-07 / IS-05 compliant GPIO interface device (such as Riedel StageLink NSA-010C's 16 GPOs) to drive camera tally lights.
- **Reverse `NMOS → TSL` (`nmos2tsl`)**: Subscribes to **NMOS IS-07 Boolean GPI Senders** on hardware GPIO boxes (such as Riedel StageLink NSA-010C's 16 `GPInput 1..16` channels) over a single multiplexed WebSocket connection, aggregates multi-GPI Red/Green states, and transmits **TSL UMD v5.0 / v3.1 (UDP / TCP)** tally packets to production switchers (such as **SimplyLive ViBox `TSL_Input` on UDP `:9001`**) or UMD multiviewers.

### Key Features

- **Single Static Binary, Ultra-Low Footprint**: Pure `CGO_ENABLED=0` static compilation (~7–8 MB binary, resident memory ~8 MB) with an embedded offline bilingual (`中文 | EN`) Web Console. Deployable directly to any Linux server, industrial PC, container, macOS host, or Windows workstation.
- **Full-Mode TSL UMD v5.0 Ingestion (Push & Pull)**:
  - `listen` (default): Dual-stack passive listener on both **UDP and TCP `0.0.0.0:8901`**; production switchers can push tally packets directly.
  - `tcp_client` (active pull): When the production switcher acts as a **TSL Listener / TCP Server**, `TSL2NMOS` connects proactively to `IP:8901` to pull streams, featuring zero-lag sticky/half-packet defragmentation (`StreamDecoder`) and automatic 2-second reconnect.
  - `udp_client`: Periodically sends keepalive probe packets to a UDP Listener to punch holes and receive return datagrams.
  - Comprehensive support for little-endian multi-`DMSG` datagrams, ASCII & UTF-16LE multilingual source labels, raw datagrams, and byte-stuffed `DLE/STX (0xFE 0x02)` stream framing.
- **Flexible Red/Green Tally Engine**: Supports 6 mapping modes (`any_color`, `rh_pgm_lh_pvw`, `lh_pgm_rh_pvw`, `rh_only`, `lh_only`, `text_only`), **PGM-overrides-PVW priority suppression** (prevents amber mixed color), NO/NC polarity inversion, and one-click Web GUI tally overrides for live testing without a switcher.
- **Reverse `NMOS → TSL` Engine & Transmitter**:
  - Auto-discovers remote NMOS IS-07 Boolean WebSocket GPI Senders (automatically filtering out non-boolean analog flows), multiplexing all 16 GPIs onto a single WebSocket connection with 5s `health` heartbeats, 350ms REST `/state` fallback, and automatic dynamic WebSocket port re-discovery after hardware reboots.
  - Encodes outbound **TSL UMD v5.0** or **v3.1** over **UDP Unicast** (e.g. SimplyLive `TSL_Input` `:9001`), **TCP Client**, or **TCP Server**, supporting SimplyLive `lh_red_rh_green`, classic `rh_red_lh_green`, and `both_color` lamp mappings, periodic state refresh, NO/NC polarity inversion, and Web UI live force-ON/OFF testing without shorting physical GPI pins.
- **Standard AMWA NMOS Protocol Stack**:
  - **IS-04 v1.2 / v1.3**: Node API (Sources, Flows, Senders, and virtual Receivers), deterministic UUIDv5 persistence, mDNS `_nmos-node._tcp` advertisement, and auto/static Registry registration with 5s heartbeats.
  - **IS-05 v1.0 / v1.1**: Single and Bulk Connection Management for both Senders (`TSL → NMOS`) and Receivers (`NMOS → TSL`), supporting immediate and scheduled activation.
  - **IS-07 v1.0 (WebSocket & REST)**: Concurrent multi-receiver subscriptions, instant Late-Joiner state sync, 12-second `health` watchdog, and graceful `shutdown` notifications.
- **Built-in IS-05 Direct Controller**: Operates standalone even **without an external NMOS Registry or Broadcast Controller**. Inspect and probe target NMOS GPIO devices via the embedded Web GUI, auto-patch odd/even Red/Green GPOs or GPIs in one click, and automatically self-heal routes after remote device power-cycles.

### Quick Start

#### 1. Compile
```bash
make build
# Cross-compile for Linux ARM64, Windows x64, and macOS (Apple Silicon / Intel):
make build-linux-arm64 build-windows-amd64 build-darwin-arm64 build-darwin-amd64
```

#### 2. Run
```bash
./bin/TSL2NMOS -config configs/config.yaml
```
*If `configs/config.yaml` does not exist upon first launch, the gateway automatically generates a default 8-channel (16 Red/Green event flows) configuration.*

#### 3. Configure via Web Console (`中文 | EN`)
- Open `http://<Host-IP>:8080/` in a web browser (click **`中文 | EN`** in the top-right header to switch languages anytime);
- **For `TSL → NMOS` (GPO Output)**: Open the **"GPO Crosspoint Patching (TSL→NMOS)"** tab, enter the target NMOS GPIO device IP (e.g., `192.168.42.59`), click **"Probe GPOs & Add Device"**, click **"Auto Odd/Even Patch (R/G)"**, and click **"Save & Activate Patch"**.
- **For `NMOS → TSL` (GPI Input → Switcher)**: Open the **"NMOS → TSL Reverse Gateway (GPI In)"** tab, probe your NMOS GPI device, click **"Auto 8-Cam Odd/Even (R/G)"** or **"Auto 16-Ch 1:1 Red"**, set the target switcher `IP:Port` (e.g., SimplyLive `192.168.42.15:9001`), enable outbound TSL, and click **"Save & Apply Immediately"**.

#### 4. Run Test Suite
```bash
make test-race
```

---

<a name="chinese-readme"></a>
## 中文说明

`TSL2NMOS` 是一款专为现代全 IP 化（SMPTE ST 2110 / AMWA NMOS）演播室设计的超轻量、零外部依赖**双向**协议转换网关（内置 **中/英文 (`中文 | EN`) 实时切换 Web 控制台**）：
- **正向 `TSL → NMOS`**：将导播切换台输出的 **TSL UMD v5.0 (UDP / TCP，支持被动接收与主动拉流)** 红绿 Tally 信号毫秒级转换为 **AMWA NMOS IS-07 (Event & Tally over WebSocket)** 布尔事件流，直接驱动任何兼容 NMOS IS-07 / IS-05 的硬件 GPIO 接口设备（如 Riedel StageLink NSA-010C 的 16 路 GPO）输出物理继电器干接点信号点亮摄像机 Tally 灯。
- **反向 `NMOS → TSL` (`nmos2tsl`)**：单连接多路复用订阅硬件接口箱上的 **NMOS IS-07 Boolean GPI 输入**（如 Riedel StageLink NSA-010C 闲置的 16 路 `GPInput 1..16`），实时聚合并编码为 **TSL UMD v5.0 / v3.1 (UDP / TCP)** 报文反向推送给 **SimplyLive ViBox (`TSL_Input` 默认 UDP `:9001`)** 或其他导播切换台/UMD 屏幕。

### 核心特性

- **单一静态二进制，极致轻量**：`CGO_ENABLED=0` 纯静态编译（约 7~8 MB，常驻内存 ~8 MB），内嵌离线中英双语 Web 控制台，拷入任意 Linux 服务器、工控机、容器、macOS 或 Windows 导播机即可运行。
- **全模式 TSL UMD v5.0 接入（推/拉双支持）**：
  - `listen`（默认）：同时开启 **UDP + TCP `0.0.0.0:8901`** 双栈被动监听，导播台向本机主动推送即可；
  - `tcp_client`（主动拉取模式）：当导播服务器配置为 **TSL Listener / TCP Server** 时，`TSL2NMOS` 主动连接导播台 `IP:8901` 拉取实时流，内置零延迟粘包/半包解帧器（`StreamDecoder`）与 **2 秒断线/未开机自动重连**；
  - `udp_client`：主动向 UDP Listener 发送周期保活帧打洞并接收回传报文。
  - 全面支持小端序单包多 `DMSG`、ASCII 与 UTF-16LE 中文源名称解码，自动兼容原生数据报与 `DLE/STX (0xFE 0x02)` 字节填充流帧。
- **灵活红/绿 Tally 判定引擎**：支持 `any_color`、`rh_pgm_lh_pvw`、`lh_pgm_rh_pvw`、`rh_only`、`lh_only`、`text_only` 六种模式，支持 **PGM 优先抑制 PVW**（防止混色黄灯）、常开/常闭极性反转以及脱离切换台的**网页一键强制红/绿灯测试**。
- **反向 `NMOS → TSL` 采集与发送引擎**：
  - 自动探测远端 NMOS 节点的布尔型 GPI WebSocket Sender（自动过滤模拟量等非布尔流），对同一 `connection_uri` 下的 16 路 GPI 共享单条 WebSocket 会话（含 5 秒 `health` 心跳与断电重启后动态端口自愈重连）；
  - 支持 **UDP 单播直发**（推荐对接 SimplyLive `TSL_Input` `:9001`）、**TCP Client** 与 **TCP Server**，支持 **TSL v5.0 / v3.1**、SimplyLive 左红右绿（`lh_red_rh_green`）编码、同 `Screen+Index` 多路红/绿自动合并、常开/反相极性及**网页免短接硬件强制测试**。
- **标准 AMWA NMOS 协议栈**：
  - **IS-04 v1.2 / v1.3**：Node API（含 Sources、Flows、Senders 及反向虚拟 Receivers）、确定性 UUIDv5 持久化、mDNS `_nmos-node._tcp` 广播以及自动/静态 Registry 注册与 5 秒心跳。
  - **IS-05 v1.0 / v1.1**：Sender 与 Receiver 单路及批量（Bulk）连接管理，支持立即激活与定时激活。
  - **IS-07 v1.0 (WebSocket & REST)**：多接收端并发订阅、新连接瞬间推送当前状态快照（Late-Joiner Sync）、严格 12 秒 `health` 心跳看门狗与优雅停机 `shutdown` 广播。
- **内置 IS-05 直连控制器**：现场即便**没有部署外部 NMOS 注册中心与广播控制器**，也可直接通过内置 Web 界面输入目标 IS-07-GPIO 设备的 IP 一键扫描其 GPO / GPI 端口，完成奇偶红/绿快速配线，并在远端设备断电重启后**自动检测并恢复路由**。

### 快速上手

#### 1. 编译
```bash
make build
# 跨平台编译（Linux ARM64 树莓派/工控机、Windows 导播主机、macOS Apple Silicon/Intel）:
make build-linux-arm64 build-windows-amd64 build-darwin-arm64 build-darwin-amd64
```

#### 2. 启动运行
```bash
./bin/TSL2NMOS -config configs/config.yaml
```
*首次运行时若 `configs/config.yaml` 不存在，程序会自动生成默认 8 路讯道（16 路红/绿事件流）配置。*

#### 3. 打开 Web 控制台配线（支持右上角 `中文 | EN` 一键切换）
- 浏览器访问 `http://<本机IP>:8080/`；
- **正向 `TSL → NMOS`（驱动 GPO 继电器）**：切换到 **【GPO 交叉点绑定 (TSL→NMOS)】** 标签页，输入目标设备 IP 点击 **【探测 GPO 并加入配置】**，点击 **【一键奇偶配线 (红/绿)】**，再点 **【保存并立即激活路由】**。
- **反向 `NMOS → TSL`（采集 GPI 喂给导播台）**：切换到 **【NMOS → TSL 反向网关 (GPI 输入)】** 标签页，探测或一键复用已配设备地址拉出 16 路 `GPInput`，点击 **【一键 8 机位奇偶红/绿】** 或 **【一键 16 路独立红 Tally】**，在上方点击 **【填入 SimplyLive TSL_Input (:9001)】** 并开启总开关，点击 **【保存并立即生效】** 即可。

#### 4. 运行测试套件
```bash
make test-race
```

---

### License

[Apache License 2.0](LICENSE)
