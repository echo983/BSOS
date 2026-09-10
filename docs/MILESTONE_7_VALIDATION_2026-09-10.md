# 里程碑 7 验证进展（2026-09-10）

状态：健康检查与运维完善（Health and operational polish: `bsosd.toml` 配置解析、CLI 标志覆盖、超时控制、运维诊断与平滑停机）已全量实现并通过本地与 VPS 宿主物理验证。

## 本轮完成

1. **TOML 配置文件支持与大小解析器 (`internal/daemon/config.go`, `config_test.go`, `bsosd.example.toml`)**：
   - 引入标准 TOML 解析支持，提供生产就绪配置模型 `fileConfig` 与加载函数 `LoadConfig(path)`。
   - 实现通用存储容量解析器 `ParseSize`，支持 `B`、`KB/K`、`MB/M`、`GB/G`（基于二进制 $1024$ 换算），严格防范越界与格式错误。
   - 字段边界约束防护：校验 `chd_target_p` $\in (0, 1]$、`trim_threshold_ratio` $\in [0, 1]$、超时参数 $> 0$、阈值关系 `trim_min_threshold_bytes <= trim_max_threshold_bytes` 等。
   - 根目录下提供完整的配置模板 `bsosd.example.toml`。

2. **CLI 标志显式覆盖机制 (`cmd/bsosd/main.go`)**：
   - 支持 `-config` 载入配置文件。
   - 结合 `flag.Visit` 实现“命令行显式传入的参数优先覆盖配置文件”的标准运维语义，支持 `-pan`、`-grpc-listen`、`-max-put`、`-small-file-pow2`、`-chd-target-p`、`-zram-snapshot-dir`、`-trim-interval`。

3. **运维状态与诊断接口 (`internal/daemon/server.go`)**：
   - 提供 `DiskCount()`、`DiskIDs()`、`Degraded()` 诊断方法。
   - 强化 `Health` RPC：不仅检查分层降级状态与索引故障，还实时判定各磁盘的物理可写空间（`canWrite(d, 1)`），在单盘故障、空间耗尽或降级时准确返回 `Ok: false`。

4. **优雅平滑停机 (Graceful Shutdown, `server.go`, `main.go`, `server_lifecycle_test.go`)**：
   - `Server.Close()` 联动底层 `grpcServer.GracefulStop()`，先停止接纳新连接，等待在途 RPC 排水，停止 Trim 定时调度器，最后原子持久化索引并关闭各底层物理磁盘。
   - `cmd/bsosd/main.go` 捕获 `SIGINT` 与 `SIGTERM` 信号，触发优雅停机；针对二次信号触发强制退出防御。

## 自动化测试验证

### 1. 本地全量单元测试与竞态检测
```sh
go test -race -count=1 ./...
go vet ./...
gofmt -l .
git diff --check
```
- `internal/daemon/config_test.go`:
  - `TestParseSize`: 覆盖合法大小（B/K/KB/M/MB/G/GB）及非法格式/空值/溢出校验。
  - `TestLoadConfigValid`: 完整字段解析、波浪号路径扩展（`expandHome`）断言。
  - `TestLoadConfigInvalidBounds`: 对负超时、越界概率、倒置 Trim 阈值等异常配置拦截测试。
- `internal/daemon/server_lifecycle_test.go`:
  - `TestServerHealthAndDegraded`: 验证健康检测在正常、降级、索引故障、空间耗尽场景下的行为及诊断方法。
  - `TestServerServeAndGracefulStop`: 启动真实 TCP 端口监听，通过 gRPC 客户端发起 Health 探测，调用 `srv.Close()` 后验证 `Serve` 0 退出码平滑退出。
- 全仓并发与竞态检测：全部通过无竞态。

### 2. 专用 Debian 13 VPS 真实部署与物理验证
在专用 Debian 13 验证节点（`vmi3340548`）上执行实机闭环验证：
1. **测试服务切换为 `-config` 模式**：
   - 部署新构建的 `bsos` 与 `bsosd` 到 `/usr/local/lib/bsos-test/`。
   - 生成生产配置文件 `/var/lib/bsos-test/bsosd.toml`。
   - 更新 systemd 单元 `bsos-test.service` 为 `ExecStart=/usr/local/lib/bsos-test/bsosd -config /var/lib/bsos-test/bsosd.toml`。
2. **在线重载与读写验证**：
   - `systemctl restart bsos-test.service` 启动，日志确认正常加载 2 块磁盘并监听 TCP 端口 `127.0.0.1:19090`。
   - 运行 `vps-smoke -mode verify`，通过 gRPC 读取小对象（zram）、大对象（常规盘）、别名对象（zram）及 Bonnie RPC（`ch_d_pow2: 26`），所有数据与 SHA-256 校验 100% 匹配。
3. **真实 SIGTERM 优雅停机信号验证**：
   - 执行 `systemctl stop bsos-test.service`。
   - 检查 `journalctl` 系统日志：
     ```text
     bsosd[28580]: 2026/09/10 21:14:13 bsosd: received signal terminated, shutting down...
     systemd[1]: Stopping bsos-test.service - BSOS integration test daemon...
     systemd[1]: bsos-test.service: Deactivated successfully.
     systemd[1]: Stopped bsos-test.service - BSOS integration test daemon.
     ```
   - 证明 `bsosd` 在收到 `SIGTERM` 后优雅处理退出并返回状态码 0，系统服务干净停止。

## 里程碑 7 验收结论

- `bsosd.toml` 配置文件支持、参数边界校验、CLI 覆盖机制、健康与诊断增强、优雅退出机制全部交付完毕。
- 本地 `-race` 与 VPS 生产环境物理服务双重验证通过。
- **里程碑 7 验收正式闭环。**
