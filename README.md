# SelfIPCs

面向同机 Linux 进程的 **C++17 共享内存 IPC 库**，用于连续状态、离散任务和大块数据传递。

## 核心能力

| 模块 | 公共接口 | 用途 |
|---|---|---|
| SPSC 状态通道 | `Producer<T>` / `Consumer<T>` | 三槽位 latest-value，读取最新控制目标、设备状态或周期快照 |
| MPMC 有界队列 | `MpmcQueue<T>` | 固定容量 FIFO，多进程任务分发，支持即时操作和 deadline 等待 |
| 固定块缓冲池 | `SharedBufferPool` | 4/16KiB 共享内存缓冲区，通过 `BufferHandle` 跨进程传递 |

SPSC 支持 POSIX shm、memfd + SCM_RIGHTS 和共享槽位的原地读写。
可选 eventfd / driver 通知将 SPSC 新样本提示接入应用的 epoll，与 timerfd、停止信号或其他 Linux FD 一起等待。
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

比较工具 `self_ipcs_shm_bench` 及其 producer/consumer 支持 `--notification futex|eventfd|driver`，默认 futex。
驱动在相同 payload、CPU、节拍、预热和采样目标下替换 SHM 等待方式，保留 Socket 与 poll 对照：
两种等待方式都通过继承的启动 socket 握手，在消费者完成注册和 CSV 准备后开始发布；每个运行使用独立通道名。

```bash
build/selfipcs-release/tools/self_ipcs_shm_bench --smoke --notification eventfd --list
build/selfipcs-release/tools/self_ipcs_shm_bench --smoke --notification eventfd \
  --only smoke-shm-eventfd-64B-same-100-controlled --out-dir /absolute/path/to/new-evidence
```

每次采集使用独立输出目录；下面的历史性能表保留 2026-10-13 的 futex 配置与采集版本。
每轮 SHM（futex/eventfd/driver/poll）或 Socket 的两个 worker 共用一个绝对截止时间：工作预算加 `17s` 启动余量，
覆盖输出排空和进程退出；超时后清理该 worker 进程组并将运行标记为失败。

2026-10-21 Linux 虚拟机上的 eventfd 基础版本 `cd9256f`（工具修正 `58c0551`）验证覆盖
注册前/中发布、drain 交错、通知合并、EINTR、eventfd 饱和、
提交后通知失败、旧 FD 替换、进程退出和实例重建。Debug 完成 252 项启用测试，ASan/UBSan
完成 80 项相关测试（`detect_leaks=0`）；Release 的新旧包与混合入口完成 6 组独立安装消费，
ASan/UBSan 的新旧通知示例完成 2 组。比较入口以相同条件的 50 样本 smoke 校验启动握手、
序号及分位数，eventfd 的正式性能采集单独进行。

### 可选驱动通知后端

驱动后端沿用同一共享内存数据面与通知对象：

```cpp
producer.enable_event_notifications(self_ipcs::NotificationBackend::kDriver);
auto notification = consumer.event_notification(self_ipcs::NotificationBackend::kDriver);
```

两端显式选择同一后端；原无参接口继续选择 eventfd。启用 driver 时打开
`/dev/self_ipcs_notification`，设备缺失或权限不足返回 `TransportFailed` 和对应 errno。
切换后端先禁用原服务，再启用新服务。`EventNotification::backend()` 返回注册使用的后端。

- sidecar v1 对应 eventfd，v2 对应 driver；保持原记录尺寸、身份与实例校验。新端收到后端不匹配的
  明确拒绝时返回 `TransportFailed/EPROTONOSUPPORT`，旧端沿原注册截止时间结束交换。
- 每次注册创建一个驱动 open-file-description，通过 SCM_RIGHTS 把同一个对象传给 consumer。
  接收端核验字符设备、设备号、读写/非阻塞/CLOEXEC 标志及 `GET_INFO` 的 ABI/能力。
- driver 的写入先复制 8B 用户参数，再用短状态锁累加；读侧串行化覆盖复制和确认，写入与 STOP
  独立于读侧互斥锁。正常读写争用保持注册有效，`GET_INFO` 明确要求 independent-writes 能力。
  计数溢出 `EOVERFLOW` 保留原积压并单独报告，真实写入错误明确返回。
  发布提交始终保留成功结果，通知失败记录 errno 并关闭控制连接唤醒接收端。
  driver drain 遇到 `EAGAIN` 可以是暂忙，应用继续检查最新共享样本并保留 level-triggered 监听。
- SDK 析构、局部取消和禁用服务按引用/控制连接收尾；原始 UAPI `STOP` 是共享通知对象的持久停止。
  SDK 保留该全局操作给显式使用 UAPI 的调用方。应用始终先 DEL 外层 epoll 注册，再释放通知对象。

安装后的 `notification_demo --backend driver` 及 `async_consume_demo --backend driver`
提供真实 fork/exec、独立打开通道和生命周期演示；默认参数使用 eventfd。
2026-10-22 已完成 SDK/模块编译及设备缺失拒绝路径检查，驱动运行演示随模块加载后验收。

### 可选 C++20 协程消费

`SELFIPCS_BUILD_ASYNC=ON` 构建独立的 Boost.Asio 组件；核心和默认 SDK 保持 C++17。
组件使用 Boost ≥ 1.74 的 descriptor、strand、steady_timer 和 awaitable：

```bash
cmake -S . -B build/selfipcs-async -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=OFF -DSELFIPCS_BUILD_ASYNC=ON
cmake --build build/selfipcs-async
cmake --install build/selfipcs-async --prefix /your/async-prefix
cmake -S examples/async_consume_demo -B /tmp/selfipcs-async-demo \
  -DCMAKE_PREFIX_PATH=/your/async-prefix
cmake --build /tmp/selfipcs-async-demo
/tmp/selfipcs-async-demo/async_consume_demo
```

下游使用 `find_package(SelfIPCs CONFIG REQUIRED COMPONENTS async)` 和
`SelfIPCs::async`，头文件为 `self_ipcs/async/consumer.hpp`。普通
`find_package(SelfIPCs CONFIG REQUIRED)` 只加载 C++17 核心依赖。

- `self_ipcs::async::Consumer<T>::create(executor, std::move(consumer))` 同步完成有界通知注册；
  在启动事件循环前准备句柄。适配器拥有 Consumer、通知对象和供 Asio 监听的聚合 FD 副本。
  选择驱动时使用 `create(executor, std::move(consumer), NotificationBackend::kDriver, timeout)`，
  复用相同 strand、单次在途等待及取消完成回调回收流程。
- `async_wait_latest(steady_clock::time_point)` 返回拷贝后的 `Result<Sample<T>>`。
  多个独立通道共享一个 `io_context`；每个句柄通过 strand 串行处理，最多一个在途等待。
- 数据读取沿用 `drain → try_read_latest`，包括 producer 退出时最后已提交的样本。
  deadline 为单一绝对时间；数据、timeout、cancel、stop 竞争由同一串行状态机选出一次结果。
- `cancel()` 取消当前等待，随后可以重新等待；`stop()` / `close()` 为持久关闭。
  取消和关闭返回携带 `ECANCELED` 的错误，超时返回 `Timeout`，重复在途操作返回
  `ConcurrentHandleUse`。FD/timer 取消回调收齐后才完成当前操作并允许下一次等待。
- 移动转移句柄所有权，析构请求关闭。应用保持事件循环运行，直到任务和关闭回调处理完毕，
  再销毁执行器；关闭完成时释放底层 Consumer，允许重新打开同一通道。
  等待过程持有共享操作状态，槽位借用在每次拷贝读取结束时释放。
- `co_await consumer.async_close()` 请求持久关闭，并等待已接受的数据等待、FD/timer 取消及
  结果投递回调收尾，释放通知和底层 Consumer 后返回。多个关闭等待者及重复关闭均可等待同一
  关闭过程；移动后旧包装器返回 `StaleHandle`。事件循环持续运行即可在返回后重新 open。
  同步 `close()/stop()` 只请求关闭，应用保持执行器运行直至异步关闭完成。

通知和协程入口服务于 SPSC；Queue/Pool 的等待使用 futex。多个通道共享消费侧 Asio 循环，
生产侧每个启用通知的通道各有一个控制服务线程。

`examples/async_consume_demo/` 以真实 `fork/exec` producer 演示两个独立通道共用一个事件循环，
并展示 timeout、cancel、重新等待和 close。增加 `SELFIPCS_DEMO_USE_LEGACY=ON` 可组合旧核心包入口。

安装后可通过下列入口复制示例、重定位 SDK，并串行编译运行基础 C++17、Queue/Pool、通知及
可选协程的新旧消费入口；消费工程只链接复制后的安装包：

```bash
python3 -B scripts/check_optional_sdk.py --prefix /your/async-prefix \
  --output-root /absolute/existing/evidence-directory --with-async
# 对应 ASan/UBSan 安装包增加 --sanitizers，沿用 detect_leaks=0。
```

### Linux 混合 I/O 与软件通知实验

`examples/mixed_io_demo/` 将 SPSC 聚合通知 FD、SocketCAN、timerfd 和停止 eventfd 放在同一个
level-triggered epoll 消费循环中。每轮按有界批次处理各来源，先读取最终样本再处理终止状态，
移除 epoll 监听后关闭 FD。示例接受专用 CAN 接口，通过真实 `fork/exec` 独立打开通道。
通道通过 owner 校验接口释放，控制 journal 按核心策略保留，供后续身份与恢复检查。
vcan 场景、物理 CAN 硬件和正式性能分别记录对应运行环境。

```bash
cmake -S examples/mixed_io_demo -B /tmp/selfipcs-mixed-demo \
  -DCMAKE_PREFIX_PATH=/your/prefix
cmake --build /tmp/selfipcs-mixed-demo
# 新建专属临时 user/network namespace 与 vcan，保存独立运行日志
python3 scripts/run_mixed_io_demo.py --binary /tmp/selfipcs-mixed-demo/mixed_io_demo \
  --output-root /tmp
```

独立实验模块位于 `kernel/notification/`，通过 miscdevice 提供每个 open-file-description
一个软件通知对象；`dup` 和 `SCM_RIGHTS` 共享该对象。普通 SDK 构建与使用保持原有路径。

```bash
# 在全新输出目录编译当前内核模块和用户态 probe；脚本仅构建
python3 scripts/build_software_notification.py --output-root /tmp
# headers 使用另一版本 GCC 时增加 --kernel-cc /absolute/path/to/matching-gcc
# 使用脚本打印的 OUTPUT 绝对目录，由具备模块管理权限的用户执行
python3 scripts/manage_software_notification.py status
sudo python3 scripts/manage_software_notification.py load \
  --module /absolute/OUTPUT/module/self_ipcs_notification.ko
sudo /absolute/OUTPUT/self_ipcs_notification_probe --device /dev/self_ipcs_notification
# 全部使用方退出并关闭 FD 后，正常卸载
sudo python3 scripts/manage_software_notification.py unload
```

UAPI 在 `kernel/notification/include/uapi/`，安装入口为 `self_ipcs/notification/driver_uapi.h`。
只读 `GET_INFO` 返回固定 32B 的 ABI 1 和 counter/STOP/independent-writes 能力；SDK 按此核验
模块接口。能力集合变化会明确拒绝旧模块或旧 SDK 的不匹配组合，更新时同时部署 SDK 和模块。
`read` / `write` 使用本机字节序的固定 8B 无符号计数，
每次成功读取确认当前全部积压；`EFAULT` 保留积压，计数溢出返回 `EOVERFLOW` 并保留原值。
`O_NONBLOCK` 无可读通知或其他读取占用读锁时返回 `EAGAIN`；写入与 STOP 使用短状态锁。
阻塞读取可由信号中断并返回 `EINTR`。读侧快照在成功复制后扣除，并发新增通知保留；
复制失败 `EFAULT` 保留全部积压，溢出检查包含尚待确认的读快照。
`SELF_IPCS_NOTIFICATION_STOP` 为持久停止：唤醒等待者，先排空已有计数，再返回 EOF / poll HUP；
停止后的写入返回 `EPIPE`。模块以 `THIS_MODULE` 持有引用，最后一个 file 引用释放时回收对象。
设备默认权限 `0600`，加载受当前系统模块权限和签名策略管理。
管理脚本校验正在运行的 kernel、headers、模块名与 vermagic，卸载前检查引用数并使用普通
`rmmod`。构建入口仅编译；加载和卸载由具备权限的操作者显式执行。

### 普通服务账号部署

`examples/driver_service/` 提供 udev `0660` 指定组、systemd 服务和 `/run/user/<uid>` 的
tmpfiles `0700` 模板。管理员先创建专用普通账号 `selfipcs-demo` 和设备组 `selfipcs-notify`，
加载与当前内核匹配的模块，使用独立安装的 SDK 构建并安装 `notification_demo` 到
`/opt/selfipcs-demo/bin/notification_demo`，再生成配置：

```bash
python3 scripts/prepare_notification_service.py --user selfipcs-demo \
  --device-group selfipcs-notify --executable /opt/selfipcs-demo/bin/notification_demo \
  --output /tmp/selfipcs-service-config
```

生成器只写指定的新目录。管理员审核后显式把 `.rules`、`.conf`、`.service` 分别安装到
`/etc/udev/rules.d/`、`/etc/tmpfiles.d/`、`/etc/systemd/system/`，执行
`systemd-tmpfiles --create /etc/tmpfiles.d/self-ipcs-notification.conf`、
`udevadm control --reload-rules`、`udevadm trigger --action=add /sys/class/misc/self_ipcs_notification`、
`systemctl daemon-reload` 和 `systemctl start self-ipcs-notification.service`。
服务以指定普通账号和补充设备组运行，设备缺失 `ENOENT`、权限不足 `EACCES` 保留原 errno。
结束时先停止服务、关闭全部设备引用，再正常卸载模块；撤销配置后重新加载规则、服务配置，
由管理员恢复设备 `root:root 0600`。运行目录的 UID、属主和 `0700` 同通道创建账号一致。

有限对照入口 `scripts/run_notification_matrix.py` 与独立验证器
`scripts/verify_notification_matrix.py` 固定 64B、100/1000Hz、同核/跨核，每后端各三轮。
默认选择 futex/eventfd；增加 `--backends futex,eventfd,driver` 生成三后端 36 轮配置，
验证器使用同一显式选择检查范围，并独立核验规范配置与 raw 中的实际后端。
显式请求 driver 时，设备缺失以具体错误保留为失败轮次。
保存 raw worker、CSV、工作区间及完整进程生命周期 CPU。CPU 百分比按各进程工作区间的
user+system CPU / 实际墙钟时间计算；生产进程包含通知控制线程。

2026-10-22 Linux 虚拟机实测（采集提交 `7063a03`）：Ubuntu 22.04、Linux 6.8、GCC 11.4，
8 vCPU VMware，Release `-O3 -DNDEBUG`、sanitizer/failpoint OFF。64B、1P1C、每轮发布1000条、
预热100个观察样本，100/1000Hz × 同/跨 vCPU × futex/eventfd × 3轮，共24轮串行采集。
独立验证确认24轮发布/观察账本及数据完整性；16轮满足零跳样、900个测量样本及零跨发布排队的
延迟比较条件。1000Hz下8轮出现 latest-value 合法序号跳跃，按观察计数保留原始记录。

下表为100Hz全部合格配对的三轮 median [min–max]，计时为同机
`CLOCK_MONOTONIC_RAW` 的 publish→observation，预热后 nearest-rank 分位数：

| vCPU / 通知 | p50（µs） | p99（µs） |
|---|---:|---:|
| 0 / futex | 14.290 [13.787–14.325] | 50.050 [43.117–94.486] |
| 0 / eventfd | 50.869 [48.419–53.555] | 117.062 [115.263–124.456] |
| 0→1 / futex | 44.668 [43.510–48.016] | 121.060 [109.982–137.944] |
| 0→1 / eventfd | 80.104 [77.719–80.941] | 182.743 [173.776–196.446] |

本批同条件下 futex 的延迟和 CPU 更低；FD 通知提供接入 epoll/Asio 统一事件循环的能力。
同批真实 vcan 使用专属临时 user/network namespace：正常/256帧突发、每组2秒、新旧安装
入口各两组，Release 4组及 ASan/UBSan 4组完成，记录各事件计数、最大处理间隔和停止时延。
另一次502.1ms业务空闲窗口中，timerfd继续运行，consumer占用0.608ms进程CPU（0.121%），
consumer为1线程、producer含控制服务共2线程。CPU通过同UID进程的POSIX CPU clock读取。
持续可读饱和、stop多源同时就绪和混合循环内producer退出场景继续按独立用例验收；驱动实测
及调试内核验证随设备与权限准备后进行。

```bash
python3 -B scripts/run_notification_matrix.py --binary-dir build/selfipcs-release/tools \
  --git-dir "$PWD" --output-root /absolute/existing/evidence-directory \
  --cpu-a 0 --cpu-b 1 --backends futex,eventfd
python3 -B scripts/verify_notification_matrix.py \
  --input-root /absolute/path/to/notification-formal-RUN \
  --output /absolute/path/to/new-verification.json
```

### 队列关闭、批量与 Pool 等待

MPMC 和 SharedBufferPool 新建实例使用 **ABI 3.0**；新旧库在 `open` 时按 major 显式拒绝
ABI 2.0/3.0 混用，返回 `AbiMismatch`。升级时先由原 creator 按所有权排空、释放并移除旧实例，
再创建新实例。SPSC 布局、`BufferHandle` 的 88B 结构、共享对象命名和身份校验沿用现有契约。
本分支使用 CMake/安装包 **0.5.0 本地候选版本**，对应 ABI 3、可选通知后端及 C++20 async 组件；
具体实现与验收由源码提交和产物 SHA 标识。历史已发布 `v0.4.0` 及其 ABI 2 内容、下载产物和
验收记录保留原归属。应用升级时按上述实例迁移顺序切换队列与 Pool。

- **全局关闭**：仅队列 creator 进程的匹配身份可以调用 `close()`。关闭与 push 共用 enqueue gate：
  已获 gate 的 push 完整提交后，close 才能提交关闭；之后的 push 返回 `Closed`。close 遇到活动 gate
  返回现有的 `QueueContention` / `ConcurrentHandleUse`，由应用选择有界重试；死亡或身份不明继续
  `RecoveryBlocked`。close 成功唤醒等待方，已提交元素继续 FIFO pop，排空后返回 `Closed`。
  析构只释放本地句柄，`remove_if_creator()` 负责按身份删除实例，三者分别调用。
- **单次等待取消**：C++17 `CancellationSource` / `CancellationToken` 使用进程本地共享状态；复制的
  source/token 共享一次性取消，token 保持状态存活；source 析构只释放引用，重新开始使用新的 source。其他普通线程可调用
  `request_cancel()`；信号处理与跨进程传递使用各自的应用层通知。fork 后独立 open 句柄，exec 后
  创建本进程 token。新增错误 `Closed`、`Cancelled` 追加在既有错误码之后。
  同一取消状态可影响多个等待/批量操作；限定单个调用时为它创建独立 source。取消保留已成功
  提交或取得的数据，队列全局 close 单独控制。
- **等待结果**：`wait_push_until(value, deadline, token)`、`wait_pop_until(deadline, token)` 和
  `wait_acquire_write_until(deadline, token)` 使用单一 steady-clock 绝对时限。每次唤醒、超时和取消
  都重新检查实际操作；已提交/取得的数据优先成功，永久错误与 `Closed` 优先于取消，仍需等待时
  `Cancelled` 优先于 `Timeout`。每轮以实际尝试为判定点，随后发生的并发发布留待下一次操作。
  等待期间保持句柄、输出和执行线程的对象存活。
- **有界批量**：`try_push_batch(values, count, token)` / `try_pop_batch(values, count, token)`
  返回 `BatchResult{count, reason, error}`，最多 256 个元素，零长度完成。成功前缀始终保留，
  `reason` 区分 Complete、Full、Empty、Closed、Cancelled、Contention、Error；`error` 在提前终止时有效。
  每个元素独立提交、释放 gate 并通知，其他批次可以交错，FIFO 顺序按成功入队元素定义。
  256 限定单次元素数，队列 capacity 单独配置；逐元素同步的吞吐和 CPU 由同条件测量记录。
  批次在开始下个元素前检查取消；成功前缀仍可消费，整批原子性由应用层组合协议提供。
- **等待空闲块**：Pool 返回 RAII `WriteBuffer`，合法 abort/归还先发布 FREE 再通知全部等待者。
  注册、epoch 快照、复查和 futex 等待覆盖检查到入睡的窗口；睡眠只保留句柄与取消注册，
  池控制锁和待归还块可供其他参与方使用。
  进程退出、代次不符与所有权不明沿原 fail-closed 路径处理。
  本地映射准入计数覆盖等待、取消注册、元信息访问及 WriteBuffer/ReadBuffer 的资源期。
  `remove_if_creator()` 与新准入原子互斥，有在途访问或借用时返回 `BufferPoolContention`；
  删除失败保持映射。停机顺序为停止新申请、取消等待、等任务退出、归还块、按身份删除。
  本地保护覆盖同一 handle；跨进程停机由应用协调，其他进程 unlink 名称仍保留本进程映射。

### Pool 与 Queue 组合发送

`self_ipcs/buffer/pool_queue.hpp` 的 `send_buffer_until(write, queue, deadline, token)`
发布 WriteBuffer 并有界投递描述符，底层 Pool/Queue 继续独立使用。
`PoolQueueSendResult` 区分 `kPublishFailed`（写块仍归调用方）、`kEnqueued`（消费链路归还）、
`kReturned`（入队失败且已归还）和 `kCleanupRequired`（保留描述符、原入队错误及归还错误）。
归还失败且已取得读所有权时，结果的 `pending_read` 保留该 RAII 资源，调用方可用
`release_checked()` 重试并检查错误；未取得读所有权时按描述符与 Pool 状态协调处置。
入队成功后辅助接口仅移交所有权，消费方可能立即取得块。
`examples/queue_pool_demo/` 使用新旧安装入口演示组合发送与父进程保留描述符收尾。
publish/enqueue 之间生产者退出、pop/acquire_read 之间消费者退出由应用确认参与方停止后
按实例身份归还或重建；业务 ACK、持久化和重发属于应用协议。FIFO 表达成功出队顺序，
业务完成顺序可不同；deadline 限定等待预算，业务过期拒发另检查业务时间戳。

### 可选本地诊断

队列与 Pool 句柄通过 `set_diagnostics_enabled(true)` 开启，默认关闭；`diagnostics()` 返回
`DiagnosticsSnapshot`，`reset_diagnostics()` 在句柄静止时清零。移动转移同一份统计状态，独立 open
各自计数；并发启停与快照使用本地原子，快照逐项读取。计数达到 `UINT64_MAX` 后饱和。

`push/pop/acquire_attempts` 包含 wait/batch 的内部尝试，successes 按成功元素/块计数；`wait_calls`
和 `batch_calls` 按外层调用计数，`waits` 为实际 futex 调用，Full/Empty/contention 为尝试结果，
timeouts/cancelled 为等待退出或批量取消，notifications 为本句柄发布的队列/Pool 唤醒提示。
capacity 是实例固定容量；并发在用量按原 status 观察，快照字段是分时观察值。
单条/批量及统计 off/on 对照固定 payload、容量、进程数、总消息量和亲和性，分别报告
worker 工作区间与父进程端到端吞吐、CPU、等待/竞争计数和预热后延迟。

独立 C++17 示例通过真实 fork/exec 与独立 open 演示容量等待、取消、批量前缀、关闭排空和 Pool
归还唤醒，并打印每个句柄的计数：

```bash
cmake -S examples/queue_pool_demo -B /tmp/selfipcs-queue-pool-demo \
  -DCMAKE_PREFIX_PATH=/your/prefix
cmake --build /tmp/selfipcs-queue-pool-demo
/tmp/selfipcs-queue-pool-demo/queue_pool_demo
# 旧包/头入口：另建目录并增加 -DSELFIPCS_DEMO_USE_LEGACY=ON
```

### 新入口与兼容

公共头按 `self_ipcs/{common,channel,queue,buffer,process}/` 划分，例如 `#include <self_ipcs/queue/mpmc_queue.hpp>`，C++ 使用 `self_ipcs::MpmcQueue<T>`。`self_ipcs` 是原实现命名空间的 alias，新旧入口共用 `libself_ipcs.a`，保留原有链接符号与进程间协议。

已有工程可继续使用 `find_package(EdgeRuntime CONFIG REQUIRED)`、`EdgeRuntime::edge_runtime`、`edge_runtime/...` 头路径和 `edge_runtime::`。工具安装同时提供 `self_ipcs_*` 名称和对应的 `edge_*` 兼容入口；SPSC 协议保持原值，MPMC/Pool 使用上述 ABI 3.0 版本边界。

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

历史 `v0.4.0` 可从 [Releases](https://github.com/zeng-lm-cgaga/SelfIPCs/releases) 下载四个文件后执行；
当前 0.5.0 候选按上文源码构建与安装命令集成：

```bash
sha256sum -c SHA256SUMS
tar -xzf SelfIPCs-0.4.0-linux-x86_64.tar.gz
cmake -S /path/to/your/app -B build \
  -DCMAKE_PREFIX_PATH="$PWD/SelfIPCs-0.4.0-linux-x86_64"
```

维护者在确认 `main`、`CMakeLists.txt` 版本和对应验收记录后，为发布版本创建对应标签。
发布任务仅在全部验证通过后取得写权限；已有 Release 保持原样，重复发布会报错，上传或下载复核失败时保留草稿与日志。

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

2026-10-22 Linux 虚拟机补充验证：`SharedBufferPoolIntegration.*` 的 **12 项真实跨进程用例**
在 Debug 和 ASan/UBSan 各完成 **12/12**，覆盖描述符链、角色切换、ABI/schema 拒绝、死亡持有者、
owner 提交窗口、epoch 边界及过期 reader。ASan/UBSan 使用 `detect_leaks=0`；失败/死亡持有者对象
保留只读身份与快照，健康对象按 SDK 所有权规则清理。可选 SDK CI 显式选择这 12 项用例。

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
