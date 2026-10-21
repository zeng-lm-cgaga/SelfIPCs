# SelfIPCs

面向同机 Linux 进程的 **C++17 共享内存 IPC 库**，用于连续状态、离散任务和大块数据传递。

## 核心能力

| 模块 | 公共接口 | 用途 |
|---|---|---|
| SPSC 状态通道 | `Producer<T>` / `Consumer<T>` | 三槽位 latest-value，读取最新控制目标、设备状态或周期快照 |
| MPMC 有界队列 | `MpmcQueue<T>` | 固定容量 FIFO，多进程任务分发，支持即时操作和 deadline 等待 |
| 固定块缓冲池 | `SharedBufferPool` | 4/16KiB 共享内存缓冲区，通过 `BufferHandle` 跨进程传递 |

SPSC 支持 POSIX shm、memfd + SCM_RIGHTS 和共享槽位的原地读写。
可选 eventfd 通知将 SPSC 新样本提示接入应用的 epoll，与 timerfd、停止信号或其他 Linux FD 一起等待。
缓冲池可与 `MpmcQueue<BufferHandle>` 组合：大块数据留在共享内存中，队列传递 88B 句柄。

- **同步与所有权**：进程共享 futex、MPMC 生产/消费分侧 gate、RAII 管理共享缓冲区所有权；校验 schema、实例代次和进程身份。
- **故障处理**：SPSC 支持经身份校验的崩溃恢复；MPMC/Pool 在所有权存在歧义时返回 `RecoveryBlocked`。
- **监督与诊断**：`ProducerSupervisor` 基于 pidfd 监督自身启动的 SPSC producer 子进程；`self_ipcs_shm_ctl` 查看通道状态并管理实例。

队列负责至多一次成功出队，业务执行结果与 ACK 由应用层管理；共享资源由 creator 和使用方按所有权释放。

## 快速开始

环境：Ubuntu 22.04 / Linux ≥ 5.15、GCC 11、CMake 3.22。新构建使用 `build/selfipcs-debug`、`build/selfipcs-asan-ubsan`、`build/selfipcs-release`。

Ubuntu 构建和测试依赖：

```bash
sudo apt-get update
sudo apt-get install -y --no-install-recommends build-essential cmake libgtest-dev python3
```

通道控制文件和 broker socket 使用当前用户的 `/run/user/<uid>/edgeruntime/`。登录会话通常会准备 `/run/user/<uid>`；无登录会话的 CI 或服务可先执行 `sudo install -d -m 0700 -o "$(id -u)" -g "$(id -g)" "/run/user/$(id -u)"`。

```bash
cmake --preset dev-debug && cmake --build --preset dev-debug
ctest --preset dev-debug

# Release 构建与安装
cmake --preset release && cmake --build --preset release
cmake --install build/selfipcs-release --prefix /your/prefix

# 完整流水线：Debug、ASan/UBSan、Release 安装及独立消费示例
scripts/ci.sh
```

下游通过安装包链接：

```cmake
find_package(SelfIPCs CONFIG REQUIRED)
target_link_libraries(app PRIVATE SelfIPCs::self_ipcs)
```

使用示例见 [examples/consume_demo/](examples/consume_demo/)，覆盖 channel、queue+wait 和缓冲池句柄传递。

### SPSC eventfd 通知

默认 `publish()` / `wait_latest()` 继续使用共享内存和 futex。生产者调用
`enable_event_notifications()` 后，消费者可调用 `event_notification(timeout)` 获取可移动的
`EventNotification`。该功能适用于 1P1C latest-value 通道，两种共享内存 transport 均可使用。

- **注册**：独立 Unix socket sidecar 通过 SCM_RIGHTS 传递同一个非阻塞、CLOEXEC eventfd；
  校验同 UID、SO_PEERCRED、双方 PID/启动时间/boot-id、角色 epoch、schema、generation 和 nonce。
  先挂接新注册，再发送初始提示，覆盖注册前及注册窗口内的发布。每次重新注册替换旧注册。
- **等待**：`native_handle()` 是内部 epoll FD，聚合 eventfd 和控制连接关闭事件，可作为应用 epoll 的
  `EPOLLIN` 监听项。采用 level-triggered 模式；收到提示后按 `drain()` → `try_read_latest()`
  （或 `try_loan_latest()`）→ 再等待的顺序处理。提示可合并，样本序号与遗漏数由通道读取结果提供。
  `drain()` 每次至多成功读取一次 eventfd 计数；发布与 drain 交错时，后续数据检查或保留的可读提示覆盖新值。
- **生命周期**：保持 Consumer 存活，同一句柄串行操作。应用先 `EPOLL_CTL_DEL`，再销毁通知对象；
  通知对象先移除内部 epoll 注册，再关闭各 FD。`disable_event_notifications()`、重新注册、consumer
  关闭及 producer 退出使旧注册失效；producer 异常退出也通过控制连接关闭唤醒。重连后重新注册，
  旧 FD 保留原实例绑定。fork 后通过 exec 创建独立句柄，示例包含完整的有界子进程回收。
- **完成与错误**：数据原子提交后才写 eventfd，同时保留原 futex 唤醒。饱和 `EAGAIN` 表示已有提示；
  `EINTR` 最多重试 8 次。提交成功保持 `publish()` / `WriteLoan::commit()` 成功，通知错误由
  `event_notification_error()` 提供 errno，并关闭该注册的控制连接。`drain()` 的 `ProducerOffline`
  或 `StaleHandle` 用于退出或重新注册处理；仍先检查通道中最后已提交的样本，再处理该状态。
- **时限**：注册使用一个绝对 MONOTONIC 截止时间覆盖 connect/send/recvmsg/重试；默认 `1000ms`，
  `0ms` 为一次 `20ms` 交换。服务端每 peer 独立预算 `100ms`，以 `10ms` 片段检查停止；
  启用时的控制锁获取沿用原控制面语义，实际完成时间受系统调度影响。

[examples/notification_demo/](examples/notification_demo/) 使用已安装包、真实 fork/exec 和独立 open，
演示注册前/后的发布，以及通知、timerfd、stop eventfd 的统一 epoll 等待：

```bash
cmake -S examples/notification_demo -B /tmp/selfipcs-notification-demo \
  -DCMAKE_PREFIX_PATH=/your/prefix
cmake --build /tmp/selfipcs-notification-demo
ctest --test-dir /tmp/selfipcs-notification-demo --output-on-failure
# 旧包和头入口：在另一个构建目录增加 -DSELFIPCS_DEMO_USE_LEGACY=ON
```

比较工具 `self_ipcs_shm_bench` 及其 producer/consumer 支持 `--notification futex|eventfd`，默认 futex。
驱动在相同 payload、CPU、节拍、预热和采样目标下替换 SHM 等待方式，保留 Socket 与 poll 对照：
两种等待方式都通过继承的启动 socket 握手，在消费者完成注册和 CSV 准备后开始发布；每个运行使用独立通道名。

```bash
build/selfipcs-release/tools/self_ipcs_shm_bench --smoke --notification eventfd --list
build/selfipcs-release/tools/self_ipcs_shm_bench --smoke --notification eventfd \
  --only smoke-shm-eventfd-64B-same-100-controlled --out-dir /absolute/path/to/new-evidence
```

每次采集使用独立输出目录；下面的历史性能表保留 2026-10-13 的 futex 配置与采集版本。
每轮 SHM（futex/eventfd/poll）或 Socket 的两个 worker 共用一个绝对截止时间：工作预算加 `17s` 启动余量，
覆盖输出排空和进程退出；超时后清理该 worker 进程组并将运行标记为失败。

2026-10-21 Linux 虚拟机验证覆盖注册前/中发布、drain 交错、通知合并、EINTR、eventfd 饱和、
提交后通知失败、旧 FD 替换、进程退出和实例重建。Debug 完成 252 项启用测试，ASan/UBSan
完成 80 项相关测试（`detect_leaks=0`）；Release 的新旧包与混合入口完成 6 组独立安装消费，
ASan/UBSan 的新旧通知示例完成 2 组。比较入口以相同条件的 50 样本 smoke 校验启动握手、
序号及分位数，eventfd 的正式性能采集单独进行。

### 新入口与兼容

公共头按 `self_ipcs/{common,channel,queue,buffer,process}/` 划分，例如 `#include <self_ipcs/queue/mpmc_queue.hpp>`，C++ 使用 `self_ipcs::MpmcQueue<T>`。`self_ipcs` 是原实现命名空间的 alias，新旧入口共用 `libself_ipcs.a`，保留原有链接符号与进程间协议。

已有工程可继续使用 `find_package(EdgeRuntime CONFIG REQUIRED)`、`EdgeRuntime::edge_runtime`、`edge_runtime/...` 头路径和 `edge_runtime::`。工具安装同时提供 `self_ipcs_*` 名称和对应的 `edge_*` 兼容入口；共享内存标识、ABI 版本及实例校验保持原值。

源码构建支持 `cmake --build <build-dir> --target self_ipcs` 和 `--target edge_runtime`；两种目标及源码内链接入口共用同一份核心编译产物 `libself_ipcs.a`。

`PayloadCodec` 特化在全局作用域使用 qualified 名称：`template <> struct self_ipcs::PayloadCodec<MyPayload> { /* codec members */ };`，具体编译示例见 [examples/compat_demo/](examples/compat_demo/)。

CMake 选项使用 `SELFIPCS_WARNINGS_AS_ERRORS`、`SELFIPCS_ENABLE_SANITIZERS`、`SELFIPCS_ENABLE_FAILPOINTS`，对应的 `EDGERUNTIME_*` 兼容选项同步到同一配置。首次配置同时指定冲突值会报错；重新配置时单独修改任一入口会同步另一入口。Debug 默认启用 failpoints，Release 默认关闭；ASan/UBSan preset 明确启用 sanitizer。

## CI/CD

GitHub Actions 提供三条工作流：

| 工作流 | 触发方式 | 验证与交付 |
|---|---|---|
| [ci](.github/workflows/ci.yml) | `main` push、PR、手动、每周一 | Ubuntu 22.04/24.04 上执行 Debug、ASan/UBSan、Release、安装包重定位消费、新旧 CMake 入口及源码包重建；手动默认及每周运行完整 C01–C23 崩溃矩阵 |
| [release](.github/workflows/release.yml) | 推送 `vX.Y.Z` 标签，或选定标签手动运行 | 校验 CMake 版本和 `main` 历史，复用完整 CI 和四组 benchmark，上传草稿、下载复核 SHA256 后公开 GitHub Release |
| [benchmark](.github/workflows/benchmark.yml) | 手动、每周一 | Release 下串行采集 SPSC、MPMC、Pool、持续负载四组数据，独立验证配置、身份、交付和统计完整性 |

CI 保存 JUnit、执行日志及崩溃现场 14 天；benchmark 保存环境、命令、二进制 SHA、raw worker report、CSV 和 `verified.json` 14 天。吞吐和延迟按采集环境解读，benchmark 工作流以数据完整性为通过条件。ASan/UBSan 沿用 `detect_leaks=0` 配置。

发布产物包括 `SelfIPCs-X.Y.Z-linux-x86_64.tar.gz`、`SelfIPCs-X.Y.Z-source.tar.gz`、`release-manifest.json` 和 `SHA256SUMS`。预编译包基于 Ubuntu 22.04、GCC 11/glibc 2.35，包含静态库、公共头、CMake 包和工具。源码包按代码路径白名单生成，文档仅含 README。CD 负责库安装包分发，业务服务由使用该库的应用部署。

从 [Releases](https://github.com/zeng-lm-cgaga/SelfIPCs/releases) 下载四个文件后执行：

```bash
sha256sum -c SHA256SUMS
tar -xzf SelfIPCs-0.4.0-linux-x86_64.tar.gz
cmake -S /path/to/your/app -B build \
  -DCMAKE_PREFIX_PATH="$PWD/SelfIPCs-0.4.0-linux-x86_64"
```

维护者在确认 `main` 和 `CMakeLists.txt` 版本后创建对应标签，例如 `git tag -a v0.4.0 -m 'SelfIPCs 0.4.0'`、`git push origin v0.4.0`。发布任务仅在全部验证通过后取得写权限；已有 Release 保持原样，重复发布会报错，上传或下载复核失败时保留草稿与日志。

本地复用完整发布校验：`bash scripts/ci.sh --full --package --out-dir /absolute/path/to/fresh-audit`。打包使用干净 Git checkout 和独立新输出目录。

## 配置与恢复边界

- SPSC payload 始终计算并验证校验和。`enable_payload_checksum` 保留源码兼容，仅接受 `true`；`false` 返回 `InvalidOptions`。工具支持 `--checksum 1`，关闭或其他取值会明确报错。
- `stale_timeout` 是弃用的兼容字段，仅接受原默认值 `100ms`；样本新鲜度由应用依据 `publish_boot_ns`、`sample_age` 和业务阈值判断。新鲜度与 owner 死亡判定相互独立。
- memfd 的 broker 交换及 observer 重试路径使用单个 monotonic 总截止时间；重试时限为零表示仅做一次交换，I/O 预算为 `20ms`。broker 对每个 peer 使用独立的 `100ms` 服务预算，并以 `10ms` 轮询片段检查停止；实际完成时间受系统调度影响。进程身份不可验证时，注册和 owner 接管返回 `RecoveryBlocked`。
- 创建阶段的 `fstat` 中断会有界重试；失败清理通过打开 FD 的 inode 绑定验证同名对象。身份仍不可验证时保留对象并返回 `RecoveryBlocked`，SPSC 同时保留非 idle journal，恢复需保留现场并核验归属。
- 安装包自动发现并传播 `Threads::Threads`，下游只需链接 `SelfIPCs::self_ipcs`。

## 测试与性能

2026-10-13 Linux 验证：Debug、ASan/UBSan 各完成 **180/180** 项启用测试；Debug 完整 C01–C23 崩溃矩阵 **23/23**，Release 安装后的独立消费示例通过。
执行环境为 Linux 虚拟机（`VM_ONLY`）；ASan/UBSan 配置为 `detect_leaks=0`。2026-10-07 的关键正常退出路径经 Valgrind 3.27.1 Memcheck 检查；完整崩溃矩阵通过当时的 `edge_crash_matrix` 显式执行。

2026-10-14 SelfIPCs 验证：全新 Debug 构建完成 **217/217** 项启用测试，ASan/UBSan 的工具、CLI、配置兼容及负例检查 **47/47**。Release 和 sanitizer 安装包均完成新入口、旧入口及两种包查找顺序的独立消费；双向新旧进程组合通过 channel、queue+wait 和 Pool 描述符链。公共 qualified `PayloadCodec` 特化、15 组新旧工具入口及默认子进程路径同步验证。

### 2026-10-13 跨进程 benchmark

以下为 **2026-10-13 Linux虚拟机实测**，自研工具通过真实 `fork/exec` 执行，历史采集版本为改名前的 `ec96119`。环境为 **VMware 8 vCPU、Ubuntu 22.04.5、Linux 6.8、GCC 11.4、CMake 3.22.1（`VM_ONLY`）**，Release `-O3 -DNDEBUG`，sanitizer/failpoint 均为 OFF。MPMC/Pool 使用 CPU 0–7 的默认亲和性；SPSC 对照固定 CPU 0 或 CPU 0/1。采集早期的同机 ROS2 构建活动随环境日志记录。

主配置各执行 3 轮，表格为 **median [min–max]**。吞吐使用父进程从启动 worker 到回收、读取并聚合报告的窗口，包含进程生命周期开销；CSV 输出、资源清理及分位数排序在该窗口之后执行。p50/p95/p99 使用 nearest-rank。MPMC/Pool 统计全部交付样本，SPSC/Socket 统计预热后的样本。

**MPMC**：capacity64，每生产者 20,000 条；busy 使用 yield 重试。延迟从首次构造消息计至成功 pop，包含编码及 Full/Contention/等待重试。

| Payload / 并发 | 等待策略 | 吞吐（万条/s） | p99（µs） |
|---|---|---:|---:|
| 64B / 1P1C | busy/yield | 133.38 [130.21–134.85] | 29.29 [27.75–34.82] |
| 64B / 1P1C | blocking | 136.20 [123.71–136.38] | 79.45 [42.92–93.98] |
| 64B / 4P4C | busy/yield | 79.79 [77.45–86.41] | 40.05 [39.24–43.68] |
| 64B / 4P4C | blocking | 7.72 [6.99–8.40] | 166.35 [154.92–185.67] |
| 64B / 16P16C | busy/yield | 78.90 [77.23–79.59] | 143.45 [141.43–148.42] |
| 64B / 16P16C | blocking | 53.77 [53.62–60.47] | 493.45 [466.76–494.51] |
| 4KiB / 4P4C | busy/yield | 43.97 [43.49–44.73] | 165.17 [130.62–183.11] |
| 4KiB / 4P4C | blocking | 23.87 [23.54–24.43] | 195.66 [193.83–401.58] |

16P16C、64B、busy/yield 的持续负载单轮交付 **2560 万条**：父计时 **32.914447s**，吞吐 **77.78 万条/s（47.47MiB/s）**，p50/p95/p99 为 **5.419/79.917/153.858µs**；累计 worker CPU 时间 **237.508s**。

**SharedBufferPool 描述符链**：block_count128、queue capacity64，每生产者 20,000 条；计量覆盖申请共享块、写 payload、通过 MPMC 传递 handle、独立进程读取及归还。延迟从取得块后的填充开始计至 acquire_read，块申请等待通过 PoolFull/Contention 计数记录。

| 块大小 / 并发 | 吞吐（万条/s） | Payload（MiB/s，中位数） | p99（µs） |
|---|---:|---:|---:|
| 4KiB / 4P4C | 27.64 [27.45–27.80] | 1079.72 | 19.84 [19.71–29.69] |
| 4KiB / 16P16C | 31.02 [30.57–31.77] | 1211.53 | 543.93 [538.39–554.63] |
| 16KiB / 4P4C | 11.76 [11.75–11.99] | 1836.73 | 39.03 [38.72–48.79] |
| 16KiB / 16P16C | 13.28 [12.71–13.36] | 2074.51 | 2480.51 [2450.01–2633.48] |

补充配置包括 MPMC 1P4C/4P1C 各一轮，以及 block_count4、capacity2、4P1C、消费者启动延迟 200ms 的缓冲池背压测试。40 轮可靠交付共 **31,108,000 条**，独立逐条核对 producer/sequence、raw worker report 与 CSV，发送=接收、重复=缺失=payload 校验错误=0；每轮 Pool 删除前全部共享块均已归还。

**SPSC / Unix SOCK_STREAM**：双方同为 64B、100Hz、1000 条，预热100后统计900；同核与不同核各3组配对。12轮均达到采样目标，gap、torn read、跨下一次发布的 queue overlap 均为0。实际进程亲和性与 CSV 发布节拍同步核验。

| 亲和性 / 传输方式 | p50（µs） | p99（µs） |
|---|---:|---:|
| CPU 0 / SHM + futex | 14.900 [13.470–16.219] | 34.341 [33.191–144.351] |
| CPU 0 / SOCK_STREAM | 16.127 [15.868–16.759] | 42.939 [38.195–49.079] |
| CPU 0/1 / SHM + futex | 67.169 [59.861–67.534] | 176.724 [172.316–182.261] |
| CPU 0/1 / SOCK_STREAM | 68.155 [66.414–73.750] | 178.518 [165.287–222.488] |

### 复现入口

`run_formal_bench.py` 串行执行有限矩阵，保留 source/flags/binary SHA、命令、退出码、raw worker report、CPU 及 CSV；`verify_formal_bench.py` 按独立规范核对所选 section 的配置集合、重复次数、交付身份、计数和分位数。`--require-suite` 要求四个 section 各一阶段，`--metadata-only` 仅验配置覆盖。新工具和受支持的旧归档格式均可验证。每次采集使用新目录，单次 watchdog 为120s；快速 smoke 矩阵另见 [scripts/](scripts/)。

```bash
cmake --preset release && cmake --build --preset release
OUT=/absolute/path/to/bench-evidence
mkdir -p "$OUT"
for section in spsc mpmc pool; do
  PYTHONDONTWRITEBYTECODE=1 python3 scripts/run_formal_bench.py \
    --section "$section" --output-root "$OUT" --cpu-a 0 --cpu-b 1
done
PYTHONDONTWRITEBYTECODE=1 python3 scripts/run_formal_bench.py \
  --section sustained --messages 1600000 --output-root "$OUT"
PYTHONDONTWRITEBYTECODE=1 python3 scripts/verify_formal_bench.py \
  --require-suite --input-root "$OUT" --output "$OUT/verified.json"
```

## 目录结构

- `include/self_ipcs/{common,channel,queue,buffer,process}/`：按功能划分的公共入口；`include/edge_runtime/` 保留兼容声明。
- `src/edge_runtime/`：共用的实现与私有头，保留链接符号和协议标识，统一由 `src/CMakeLists.txt` 构建。
- `tools/{include,src}/`：诊断、benchmark 和崩溃矩阵工具。
- `test/`：单元测试与跨进程集成测试。
- `examples/`：独立安装包消费示例。
- `scripts/`、`.github/workflows/`：自动化构建与验证。
