# 里程碑 5 验证进展（2026-09-10）

状态：碎片整理（Trim）、打包表（Packed Table）编解码、碎片阈值启发式算法与 Bonnie RPC 实现完成；单元测试与竞态检测（-race）全部通过。

## 本轮完成

1. **打包表编解码 (`packed_table.go`)**：
   - 实现 `packedTableHeader` 与 `packedTableEntry` 的二进制编解码器 (`encodePackedTable`, `decodePackedTable`)。
   - 保留 NBSS 打包表二进制格式，包含 32 字节头部（魔数 `PACK`、版本、disk_id、container_fid、container_size、entry_count、reserved 等）及变长条目序列。
   - 包含边界检查、截断保护与非法字段防护。

2. **碎片阈值启发式 (`fragmentation.go`)**：
   - 实现 `computeFragmentationThreshold(sizes []uint64)`：计算文件大小分布的加权 2 的幂次分布（`floorLog2`、`pow2Bytes`）。
   - 实现碎片阈值动态估计（根据小文件数量与大小分布确定打包基准阈值）。

3. **内存与磁盘索引适配 (`index_state.go`, `state.go`)**：
   - 扩展 `objectRef` 增加 `offset uint64` 与 `packed bool` 字段，实现直接对象（`offset=0`）与打包对象（`offset=containerOffset`）在底层数据读取接口（`readRange` / `Get`）上的无缝统一，零分支。
   - 在 `replayIndex` 中支持识别 `blk.IsPackedAnchor`、加载并解析打包表、恢复 `s.confirmed` 和 `s.packed` 映射，以及处理墓碑记录（`blk.IsTombstone`）。

4. **强制连续整理子系统 (`trim.go`)**：
   - 遵循 `docs/DESIGN.md` §3.8：Trim 为强制连续运行，不设任何禁用开关。
   - 候选对象严格排除 jump-alias 指针、jump-alias 目标对象、已有的容器对象以及已有的打包表。
   - 容器组装：将散碎直接小对象拼接写入临时容器，若发生磁盘数据区区间碰撞则通过尾部追加 8 字节 Nonce 重试散列，确保不破坏前部候选数据偏移。
   - 写入打包表对象并在索引流追加打包锚点对（`packed-anchor` + `real-entry`）。
   - 内存热更新 `s.confirmed` 与 `s.packed`，并校验全部打包候选对象的数据可读性与长度一致性。
   - 追加墓碑条目并执行索引流压实（`compactIndexLocked`），清理旧小对象占用的索引槽位与数据槽区间。
   - 提供 6 个崩溃故障注入检查点（`failPoint`）：`after-backup`、`after-container-build`、`after-container-write`、`after-table-write`、`after-anchor-append`、`after-delete`，保证在任何中间步骤异常时数据完全不丢失且正常读回。

5. **Bonnie RPC 与调度器 (`server.go`, `config.go`)**：
   - 在 `Config` 中加入 Trim 相关参数（`TrimInterval`, `TrimMinThresholdBytes`, `TrimMaxThresholdBytes`, `TrimMinFileCount`, `TrimThresholdRatio`, `TrimTempDir`），默认 15 分钟连续周期执行。
   - 实现 `Server.startTrimScheduler`：根据配置周期触发各有效数据盘的 `maybeTrim()`。
   - 实现 `bonnieCHDPow2()`：汇总所有可写非 zram 磁盘的最大 CH_d，受 `max_put_bytes` 约束，以 $2$ 的幂次下取整返回。
   - 暴露并实现 gRPC `rpc Bonnie(Empty) returns (BonnieResponse)` 服务。

## 自动化测试验证

运行命令：
```sh
go test -race -count=1 ./...
go vet ./...
gofmt -l .
git diff --check
```

测试结果：
- `internal/daemon/packed_table_test.go`: 编解码往返及非法头部测试通过。
- `internal/daemon/fragmentation_test.go`: 阈值计算及极端边界测试通过。
- `internal/daemon/trim_test.go`:
  - `TestTrimPacksSmallObjectsAndPreservesReads`: 105 个小对象打包、读回、冷启动重新加载重放读回，全部通过。
  - `TestTrimExcludesJumpAlias`: 验证 jump-alias 指针与 target 均被严格排除在 Trim 候选外。
  - `TestTrimExcludesPackedTablesAndContainers`: 验证容器对象与打包表自身不被二次整理。
  - `TestTrimFailureInjection`: 全部 6 个 failpoint 注入故障，数据读回与冷启动重开均完整可用。
  - `TestServerTrimScheduler`: 周期性调度及优雅关闭（Close）测试通过。
- `internal/daemon/bonnie_test.go`:
  - `TestBonnieRPCAndCHDPow2`: gRPC RPC 端到端调用与算法结果核对通过。
- 全仓并发与竞态检测：无任何 race 报错或死锁。

## 已执行：真实 VPS 部署与全量验证

在专用 Debian 13 VPS 宿主上执行实测：
1. **真实守护进程在线热重载与 Bonnie RPC 实测**：
   - 编译并部署新版 `bsosd` 到系统服务 `bsos-test.service`。
   - 运行更新后的 `vps-smoke -mode verify`，通过真实 TCP gRPC 调用 `Bonnie` RPC，成功返回 `bonnie ch_d_pow2: 26`（对应 384MB 盘的 CHD 下取整 64MB，zram 盘正确排除在外），且原有 3 种负载对象（小对象 64KB、大对象 5MB、别名对象 32KB）读回校验及 SHA-256 全部 100% 匹配。
2. **真实 zram 物理恢复门禁**：
   - `BSOS_REAL_ZRAM=1 go test ./internal/zram -run '^TestRealZramRecovery$' -count=1 -v`：耗时 4.06s，通过全部断言。
3. **VPS 全量非缓存竞态测试**：
   - `go test -race -count=1 ./...`：耗时约 21s（blk 1.023s、daemon 9.790s、zram 10.323s），全部通过无竞态。

## 里程碑 5 验收结论

- 打包表与碎片整理子系统、Bonnie RPC 完整对齐设计要求。
- Trim 保持持续强制运行，无任何禁用开关；严格排除 jump-alias 指针与目标。
- 本地与专用 VPS 双重物理验证通过，全量单元测试与 `-race` 测试通过。
- **里程碑 5 验收正式闭环。**

