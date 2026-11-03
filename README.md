# `TSL2NMOS` — TSL 5.0 (UDP/TCP) 转 AMWA NMOS IS-07 轻量级广播级网关

`TSL2NMOS` 是一款专为现代全 IP 化（SMPTE ST 2110 / AMWA NMOS）演播室设计的超轻量、零外部依赖协议转换网关。它将导播切换台输出的 **TSL UMD v5.0 (UDP / TCP，支持被动接收与主动拉流)** 红绿 Tally 信号毫秒级转换为 **AMWA NMOS IS-07 (Event & Tally over WebSocket)** 布尔事件流，直接驱动任何兼容 NMOS IS-07 / IS-05 的硬件 GPIO 接口设备输出物理继电器干接点信号点亮摄像机 Tally 灯。

## 核心特性

- **单一静态二进制，极致轻量**：`CGO_ENABLED=0` 纯静态编译（约 7~8 MB，常驻内存 ~8 MB），内嵌离线 Web 控制台，拷入任意 Linux 服务器、工控机、容器或 Windows 导播机即可运行。
- **全模式 TSL UMD v5.0 接入（推/拉双支持）**：
  - `listen`（默认）：同时开启 **UDP + TCP `0.0.0.0:8901`** 双栈被动监听，导播台向本机主动推送即可；
  - `tcp_client`（主动拉取模式）：当导播服务器配置为 **TSL Listener / TCP Server** 时，`TSL2NMOS` 主动连接导播台 `IP:8901` 拉取实时流，内置零延迟粘包/半包解帧器（`StreamDecoder`）与 **2 秒断线/未开机自动重连**；
  - `udp_client`：主动向 UDP Listener 发送周期保活帧打洞并接收回传报文。
  - 全面支持小端序单包多 `DMSG`、ASCII 与 UTF-16LE 中文源名称解码，自动兼容原生数据报与 `DLE/STX (0xFE 0x02)` 字节填充流帧。
- **灵活红/绿 Tally 判定引擎**：支持 `any_color`、`rh_pgm_lh_pvw`、`lh_pgm_rh_pvw`、`rh_only`、`lh_only`、`text_only` 六种模式，支持 **PGM 优先抑制 PVW**（防止混色黄灯）、常开/常闭极性反转以及脱离切换台的**网页一键强制红/绿灯测试**。
- **标准 AMWA NMOS 协议栈**：
  - **IS-04 v1.2 / v1.3**：Node API、确定性 UUIDv5 持久化（重启永不改变交叉点 UUID）、mDNS `_nmos-node._tcp` 广播以及自动/静态 Registry 注册与 5 秒心跳。
  - **IS-05 v1.0 / v1.1**：Sender 单路与批量（Bulk）连接管理，支持立即激活与定时激活。
  - **IS-07 v1.0 (WebSocket)**：多接收端并发订阅、新连接瞬间推送当前状态快照（Late-Joiner Sync）、严格 12 秒 `health` 心跳看门狗与优雅停机 `shutdown` 广播。
- **内置 IS-05 直连控制器**：现场即便**没有部署外部 NMOS 注册中心与广播控制器**，也可直接通过内置 Web 界面输入目标 IS-07-GPIO 设备的 IP 一键扫描其 GPO 端口，完成奇偶红/绿快速配线，并在远端设备断电重启后**自动检测并恢复 IS-05 路由**。

## 快速上手

### 1. 编译
```bash
make build
# 跨平台编译（Linux ARM64 树莓派/工控机、Windows 导播主机）:
make build-linux-arm64 build-windows-amd64
```

### 2. 启动运行
```bash
./bin/TSL2NMOS -config configs/config.yaml
```
首次运行时若 `configs/config.yaml` 不存在，程序会自动生成默认 8 路讯道（16 路红/绿事件流）配置。

### 3. 对接导播切换台与 NMOS IS-07-GPIO 设备
1. **GPIO 设备侧准备**：确认目标设备所需 GPO 端口已开启 **NMOS 模式**，且与运行 `TSL2NMOS` 的服务器网络互通。
2. **导播切换台侧准备（任选其一，无需重启即可在网页热切换）**：
   - **情况 A（导播台主动推流 Sender）**：在切换台软件中开启 **TSL 5.0 UDP（或 TCP）** 输出，目标 IP 指向运行 `TSL2NMOS` 的服务器 IP，端口 `8901`（默认 `listen` 模式）。
   - **情况 B（导播台为 Listener 等待拉取）**：在 `TSL2NMOS` 网页【讯道与 TSL 映射规则配置】将接入模式切为 **`主动拉取 TCP`**，填入导播服务器 IP（如 `192.168.42.10:8901`，只填 IP 默认自动补 `:8901`）并点击 **`保存并立即生效`**，或命令行直接用 `-tsl-mode tcp_client -tsl-remote 192.168.42.10:8901` 启动。
3. **打开 Web 控制台配线**：
   - 浏览器访问 `http://<本机IP>:8080/`；
   - 切换到 **“GPO 交叉点绑定”** 标签页，输入目标设备的 IP 点击 **“探测 GPO 并加入配置”**；
   - 选择起始讯道后点击 **“一键奇偶配线 (红/绿)”**（或按需手动下拉选择），再点击 **“保存并立即激活路由”**；
   - 回到 **“实时 Tally 矩阵 & 强制测试”** 页面，点击任意讯道的 **“强红” / “强绿”** 测试硬件继电器与摄像机 Tally 灯，无误后点 **“全部恢复自动”** 即可投入制作。

## 运行测试套件
```bash
make test
```
