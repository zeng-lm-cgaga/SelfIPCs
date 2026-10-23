# SelfIPCs

[![CI](https://github.com/zeng-lm-cgaga/SelfIPCs/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/zeng-lm-cgaga/SelfIPCs/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/zeng-lm-cgaga/SelfIPCs)](https://github.com/zeng-lm-cgaga/SelfIPCs/releases/latest)

面向同机 Linux 进程的 **C++17 共享内存通信库**，以 SDK 形式提供状态通道、任务队列和大块数据传递。

## 特性

| 组件 | 接口 | 适用场景 |
|---|---|---|
| 最新值通道 | `Producer<T>` / `Consumer<T>` | SPSC 三槽位，传递控制目标、设备状态和周期快照 |
| 有界任务队列 | `MpmcQueue<T>` | 多生产者/多消费者 FIFO，支持容量等待、取消、批量操作和关闭排空 |
| 共享内存块池 | `SharedBufferPool` | 固定 4/16 KiB 块，通过队列传递 `BufferHandle` 描述符 |

- **跨进程同步**：共享 futex、绝对截止时间等待和 RAII 资源管理。
- **实例与恢复**：校验 schema、实例重建编号和进程身份；SPSC 支持崩溃恢复，`ProducerSupervisor` 通过 pidfd 监督子进程。
- **事件循环集成**：可选 eventfd / 软件通知驱动接入 epoll，C++20 Boost.Asio 组件提供协程等待与关闭。
- **SDK 交付**：公共头、静态库、CMake 包与诊断工具，支持独立安装和外部工程集成。

## 快速开始

环境：Linux x86_64、Ubuntu 22.04 / Linux ≥ 5.15、GCC ≥ 11、CMake ≥ 3.22。

从 [v0.5.0 Release](https://github.com/zeng-lm-cgaga/SelfIPCs/releases/tag/v0.5.0) 下载 SDK 包、源码包、`release-manifest.json` 和 `SHA256SUMS`：

```bash
sha256sum -c SHA256SUMS
tar -xzf SelfIPCs-0.5.0-linux-x86_64.tar.gz
tar -xzf SelfIPCs-0.5.0-source.tar.gz

# 用 SDK 构建并运行源码包中的示例
cmake -S SelfIPCs-0.5.0-source/examples/consume_demo -B build/consume-demo \
  -DCMAKE_PREFIX_PATH="$PWD/SelfIPCs-0.5.0-linux-x86_64"
cmake --build build/consume-demo
ctest --test-dir build/consume-demo --output-on-failure
```

预编译 SDK 基于 GCC 11 / glibc 2.35；其他工具链可从源码构建。

在应用的 CMake 工程中链接：

```cmake
find_package(SelfIPCs CONFIG REQUIRED)
target_link_libraries(app PRIVATE SelfIPCs::self_ipcs)
```

公共头使用 `self_ipcs/...`，例如 `#include <self_ipcs/queue/mpmc_queue.hpp>`。
通道控制文件使用 `/run/user/<uid>/edgeruntime/`；服务或 CI 环境需准备当前 UID 所有、权限为 `0700` 的 `/run/user/<uid>`。

## 示例

| 示例 | 内容 |
|---|---|
| [consume_demo](examples/consume_demo/) | 通道读写、队列等待、共享块描述符传递 |
| [queue_pool_demo](examples/queue_pool_demo/) | 关闭排空、取消、批量操作、块池与队列组合发送 |
| [notification_demo](examples/notification_demo/) | eventfd / driver 通知与 epoll |
| [async_consume_demo](examples/async_consume_demo/) | 多通道共用 Asio 循环，协程等待、取消与关闭 |
| [mixed_io_demo](examples/mixed_io_demo/) | 通道通知、SocketCAN、timerfd 与停止事件 |
| [driver_service](examples/driver_service/) | 普通服务账号的 udev、systemd 和 tmpfiles 模板 |

前五个示例为独立 CMake 工程，通过 `-DCMAKE_PREFIX_PATH=/your/sdk/prefix` 接入安装包。

## 从源码构建

```bash
sudo apt-get install -y build-essential cmake python3 libgtest-dev
cmake --preset release
cmake --build --preset release
cmake --install build/selfipcs-release --prefix "$PWD/install"
```

### 可选组件

**协程消费**需要 C++20 和 Boost ≥ 1.74，从源码启用：

```bash
sudo apt-get install -y libboost-system-dev
cmake -S . -B build/selfipcs-async -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=OFF -DSELFIPCS_BUILD_ASYNC=ON
cmake --build build/selfipcs-async
cmake --install build/selfipcs-async --prefix "$PWD/install-async"
```

下游使用 `find_package(SelfIPCs CONFIG REQUIRED COMPONENTS async)`，链接 `SelfIPCs::async`。
`async_wait_latest(deadline)` 等待数据，`cancel()` 取消当前等待，`async_close()` 等待关闭完成；关闭期间保持事件循环运行。

**FD 通知**由 producer 调用 `enable_event_notifications()` 启用，consumer 通过 `event_notification()` 获取通知对象。
在 level-triggered epoll 中监听 `native_handle()`，就绪后按 `drain()` → `try_read_latest()` 读取；重新连接通道后重新注册。
两端可显式选择 `NotificationBackend::kDriver` 使用软件通知驱动。

<details>
<summary>构建与使用可选驱动</summary>

准备与运行内核匹配的 headers 和编译器，使用脚本输出的 `OUTPUT` 绝对目录：

```bash
python3 scripts/build_software_notification.py --output-root /tmp
sudo python3 scripts/manage_software_notification.py load \
  --module /absolute/OUTPUT/module/self_ipcs_notification.ko
sudo /absolute/OUTPUT/self_ipcs_notification_probe --device /dev/self_ipcs_notification
# 全部使用方关闭 FD 后卸载
sudo python3 scripts/manage_software_notification.py unload
```

设备为 `/dev/self_ipcs_notification`，默认权限 `0600`；普通账号部署见 [driver_service](examples/driver_service/)。
模块与 SDK 按 UAPI 能力协同升级。

</details>

## 使用约定

- **交付语义**：SPSC 读取最新状态，慢消费者通过序号观察覆盖；MPMC 按成功入队顺序出队，业务 ACK、持久化和重试由应用管理。
- **数据格式**：SPSC 通过 [`PayloadCodec`](include/edge_runtime/common/schema.hpp) 编解码；队列使用固定布局、可平凡复制的负载，大块数据通过 `BufferHandle` 传递。
- **关闭与资源**：队列 creator 调用 `close()`，已提交元素继续排空；进程停止后按所有权移除实例。等待采用绝对 deadline，取消保留成功结果，批量接口返回成功前缀。
- **故障处理**：Queue/Pool 遇到所有权歧义返回 `RecoveryBlocked`，保留现场，由应用协调参与进程退出并恢复或重建。
- **版本兼容**：v0.5.0 的 Queue/Pool 使用 ABI 3，升级时先排空并移除 ABI 2 实例；SPSC 布局保持。旧 `EdgeRuntime` CMake 包、头路径及工具入口继续兼容。

## 性能

Linux 虚拟机实测：VMware 8 vCPU、Ubuntu 22.04、Linux 6.8、GCC 11.4，Release `-O3 -DNDEBUG`，sanitizer/failpoint 关闭。

**SPSC 通知**（2026-10-22，`53131b1`）：64B、1P1C、100Hz，每轮发布 1000 条，预热 100 个观察样本。
每轮统计预热后的 900 个样本；下表为三轮 p99 的中位数，计时为发布到观察，采用 nearest-rank 分位数。

| 通知后端 | 同 vCPU p99（μs） | 跨 vCPU p99（μs） |
|---|---:|---:|
| futex | 36.896 | 216.589 |
| eventfd | 101.566 | 251.430 |
| driver | 119.808 | 232.913 |

**历史 MPMC 持续负载**（2026-10-13，`ec96119` / ABI 2）：64B、16P16C、capacity 64、busy/yield，交付 2560 万条，吞吐 **77.78 万条/s**，p99 **153.858μs**。
吞吐窗口包含 worker 启动和回收；延迟从首次构造消息到成功出队，包含重试。

复现入口：[队列/块池 benchmark](scripts/run_formal_bench.py) 与[结果校验](scripts/verify_formal_bench.py)、[通知后端对照](scripts/run_notification_matrix.py) 与[结果校验](scripts/verify_notification_matrix.py)。
[benchmark 工作流](.github/workflows/benchmark.yml) 串行执行 SPSC、MPMC、Pool 和持续负载，保存环境、原始数据及 CSV。

## 测试与 CI/CD

```bash
cmake --preset dev-debug && cmake --build --preset dev-debug
ctest --preset dev-debug
cmake --preset asan-ubsan && cmake --build --preset asan-ubsan
ctest --preset asan-ubsan

# 完整崩溃矩阵、安装消费和发布包校验；输出到新目录
bash scripts/ci.sh --full --package --out-dir /absolute/path/to/fresh-audit
```

[CI](.github/workflows/ci.yml) 在 Ubuntu 22.04 运行构建、单元/跨进程测试、ASan/UBSan 和安装包消费；ASan 使用 `detect_leaks=0`。
[发布流水线](.github/workflows/release.yml) 对版本标签运行崩溃恢复测试、可选组件集成及 benchmark 校验，生成 SDK 包、源码包、manifest 和 SHA256SUMS，校验下载产物后公开 Release。
本地打包使用干净的 Git checkout。
