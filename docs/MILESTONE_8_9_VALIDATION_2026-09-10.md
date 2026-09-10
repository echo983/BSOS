# 里程碑 8 与里程碑 9 验证进展（2026-09-10）

状态：**里程碑 8（Go 客户端库 `pkg/client`）** 与 **里程碑 9（基础 CLI 工具 `bsos put|get|head|bonnie|health`）** 按照 `docs/CLIENT_SPEC.md` 规范全量实现闭环，并在本地及专用 Debian 13 VPS 真实服务节点上完成物理端到端验证。

## 架构与规范对齐

严格遵守 `docs/CLIENT_SPEC.md` 定义的基础设施范围边界（Scope Boundary）：
- **零分块、零清单支持**：严禁在存储层与客户端 SDK 引入多对象逻辑切分或清单（Manifest）重组；BSOS 专精底层内容寻址字节流（"one fid per object"），高级结构完全交由上层应用。
- **原始字节流输入输出**：客户端库与 CLI 均保持“原始字节输入、原始字节输出”，以单 fid 为唯一实体。

---

## 交付成果明细

### 1. Milestone 8: Go 客户端库 (`pkg/client/client.go`, `client_test.go`)
- **唯一身份哈希计算 (`ComputeFID`)**：
  - 基于 `github.com/zeebo/xxh3` 计算 `xxh3_64(content)`，与服务端 `AddrForFID` 保持 100% 同构一致。
- **流式写入 (`Put`, `PutBytes`)**：
  - 首帧严格发送 `PutHeader{fid, total_size, alias_for}`。
  - 后续分片按 1 MiB 默认块大小流式传输。
  - **规范第 2.3 条强约束实现**：每发送一个 chunk 均即时检测流发送错误（`stream.Send`）；一旦服务端提前拒绝（如容量超限、参数非法、槽位冲突），立即终止后续所有数据发送并收取服务端真实错误码，彻底杜绝带宽浪费。
- **对象读取与范围读取 (`Get`, `GetBytes`)**：
  - 支持全量对象读取或基于 `Range{Start, End}` 的半开区间范围切片读取。
- **元数据与运维 RPC (`Head`, `Bonnie`, `Health`)**：
  - `Head`: 快速获取对象逻辑存储长度。
  - `Bonnie`: 探测存储池当前单对象最大可放置容量估值 $2^{\text{ch\_d\_pow2}}$。
  - `Health`: 检查存储池运行健康状态。
- **一跳跳转重试助手 (`PutWithJumpRetry`, 规范第 3 节)**：
  - 遇到物理槽位冲突时，自动遍历 `jumpCode 1..255`，附加 salt 字节并派生 `jumpFID`，通过 `PutHeader{fid: jumpFID, total_size: len(jumpData), alias_for: fid}` 发起一跳别名注册。
  - 成功后通过原逻辑 `fid` 读取自动去除跳转后缀，通过 `jumpFID` 可读取完整物理数据。
- **重试安全回读校验助手 (`VerifyContent`, 规范第 4 节)**：
  - 针对并发写或冲突后的重试场景，实现指数退避重试回读（容忍并发写途中的瞬态 `NotFound`），比对内容字节一致性或长度一致性。
- **错误分类判定器**：
  - `IsConflict` (`AlreadyExists`), `IsNotFound` (`NotFound`), `IsResourceExhausted`, `IsInvalidArgument`。

### 2. Milestone 9: 基础命令行工具 (`cmd/bsos/main.go`, `ops.go`, `ops_test.go`)
提供基于 `pkg/client` 的简洁 CLI 子命令，支持 `$BSOS_ADDR` 环境变量与 `-addr` 参数（默认 `127.0.0.1:9090`）：
- `bsos put [flags] [file|-]`：
  - 从指定文件或标准输入（stdin）读取字节流并写入。
  - 默认自动执行一跳跳转碰撞重试，输出最终确认的十六进制 FID（如发生跳转输出 `0x... (jumped to 0x... via jump code N)`）。
  - 支持 `-no-jump` 严格禁用跳转重试（遇冲突即退出 1）。
- `bsos get [flags] <fid> [file|-]`：
  - 读取指定 FID 内容输出至终端标准输出或目标文件。
  - 支持 `-range <start>-<end>` 范围拉取。
- `bsos head [flags] <fid>`：
  - 查询对象是否存在及其实际逻辑字节大小，支持 `-json` 输出。
- `bsos bonnie [flags]`：
  - 查询存储池最大可放置对象大小评估，支持 `-json` 输出。
- `bsos health [flags]`：
  - 检查服务端健康度，输出 `OK` 或 `DEGRADED`，支持 `-quiet` 用于系统监控探测。

---

## 自动化测试与竞态验证

### 1. 本地全量单元测试与竞态检测
```sh
go test -race -count=1 ./...
go vet ./...
gofmt -l .
git diff --check
```
- `pkg/client/client_test.go`:
  - `TestComputeFID`: 验证 FID 计算与标准 xxh3_64 一致。
  - `TestClientPutGetBytesAndHead`: 完整往返写入、读回、区间读取、Head 探测、重复写入冲突与不存在 FID 拦截。
  - `TestClientPutStreamingMultiChunk`: 128KB 小 chunk 流式传输 1.6MB 数据（12+ 分片），内容 100% 校验。
  - `TestClientEarlyRejectionStop`: 验证首帧非法时服务端早期拒绝与客户端即时停发机制。
  - `TestClientPutWithJumpRetry`: 物理槽位冲突下自动触发 jumpCode 1 成功完成一跳别名写入，原 FID 逻辑读回、物理 FID 读回、越界穷尽均满足断言。
  - `TestClientVerifyContent`: 验证回读退避重试、内容一致比对与大小比对。
  - `TestClientBonnieAndHealth`: 验证端到端 Bonnie 与 Health RPC。
  - `TestClientErrorHelpers`: 验证各错误状态码辅助方法。
- `cmd/bsos/ops_test.go`:
  - `TestCLIPutGetHeadLifecycle`: 捕获标准输入输出，端到端测试 CLI 命令 `put`、`head`、`get`、`get -range`、`bonnie`、`health`。
- **本地所有包均在 `-race -count=1` 下耗时通过，零死锁、零竞态**。

### 2. 专用 Debian 13 VPS 实机测试与全量物理验证
在专用 Debian 13 验证节点（`vmi3340548`）上同步最新代码，编译并部署 `bsos` 到系统路径：
1. **全量非缓存远程竞态检测**：
   - `go test -race -count=1 ./...`：全仓包测试耗时约 28s，全部通过。
2. **真实系统服务与 CLI 命令物理联调**：
   - `bsos health` -> 输出 `OK` (退出码 0)。
   - `bsos bonnie` -> 输出 `ch_d_pow2: 26 (max single placement estimate: 67108864 bytes)`；`-json` 输出 `{"ch_d_pow2":26,"max_bytes":67108864}`。
   - `bsos put /tmp/bsos_test_in.txt` -> 成功写入，返回 `0x2221a9d4782ebf9c`。
   - `bsos head 0x2221a9d4782ebf9c` -> `fid: 0x2221a9d4782ebf9c size: 49 bytes`。
   - `bsos get 0x2221a9d4782ebf9c /tmp/bsos_test_out.txt` -> `diff -u` 比对原始文件完全一致。
   - `bsos get -range 0-8 0x2221a9d4782ebf9c` -> 准确返回切片字符串 `BSOS-CLI`。
   - `bsos put -no-jump /tmp/bsos_test_in.txt` -> 正确拦截并打印 `E_CONFLICT: fid 0x2221a9d4782ebf9c already registered`，退出码 1。
   - 联动运行 `vps-smoke -mode verify`：zram 小对象、常规盘大对象、zram 别名对象读取与 SHA-256 全部 100% 校验通过。

---

## 验收结论

- `pkg/client` 完整交付 `docs/CLIENT_SPEC.md` 所要求的所有能力（哈希、流式 Put、早期停发、Get/Range、Head、Bonnie、Health、JumpRetry、RetrySafety）。
- `cmd/bsos` 交付生产级命令行子命令，行为与规格定义一致。
- 本地与 VPS 物理环境所有测试及竞态检测全绿通过。
- **里程碑 8 与里程碑 9 正式验收闭环。**
