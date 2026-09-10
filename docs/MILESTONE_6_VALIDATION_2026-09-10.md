# 里程碑 6 验证进展（2026-09-10）

状态：客户端驱动的一跳跳转别名（One-hop alias, `alias_for`）协议与多盘隔离、Trim 避让、崩溃恢复重放验证全部闭环；单元测试与竞态检测（-race）全部通过。

## 本轮完成

1. **协议语义完整性与端到端支持 (`alias_test.go`, `server.go`, `state.go`)**：
   - 依据 `docs/DESIGN.md` §3.4 与 `docs/CLIENT_SPEC.md` §3：服务端不介入碰撞重试，由客户端决定使用 `alias_for`。
   - 别名解析：通过 `Get(aliasFor)` 读取时，自动去除末尾追加的 1 字节跳转后缀，返回与写入前完全一致的逻辑内容；`Head(aliasFor)` 准确返回去后缀后的逻辑长度。
   - 目标直接读取：通过 `Get(targetFID)` 读取时返回包含跳转后缀的完整落盘有效载荷；`Head(targetFID)` 返回完整存储长度。
   - 支持别名对象的范围读取（`RangeStart` / `RangeEnd`），自动做逻辑长度边界截断保护。

2. **同盘物理共置保证 (Co-locality Guarantee, §3.11)**：
   - `PreparedWrite` 与 `Commit(aliasFor)` 绑定在单一选定磁盘上。
   - 保证跳转指示条目（`aliasFor, Sentinel, 1`）与实际数据条目（`targetFID, totalSize, 0`）在同一物理磁盘的索引流中紧邻写入，杜绝跨盘分散。

3. **双 FID 全池网关原子约束与非法场景防御 (§3.3 step 0, `gate.go`)**：
   - 网关对 `fid` 与 `aliasFor` 进行全池原子预留，严防普通写入与别名写入并发竞争。
   - 严格防御非法调用：
     - `alias_for == fid`：直接拒绝（`InvalidArgument`）。
     - `alias_for != 0` 且 `total_size < 2`：直接拒绝（`InvalidArgument`）。
     - 企图覆盖已存在的物理对象（`alias_for` 已存在）：直接拒绝（`AlreadyExists`）。
     - 链式别名企图（对已有别名指针或别名目标再次建立别名）：全部被原子网关拦截拒绝（`AlreadyExists`）。

4. **Trim 碎片整理严格免疫 (§3.8, `trim.go`)**：
   - Trim 候选收集算法严格识别 `record.Jump` 指示项与 `jumpTargets[record.FID]` 目标项。
   - 验证别名目标对象与指示条目在磁盘 Trim 压实时完全不被重打包，并在压实重放与冷启动后持续保持完整可读。

5. **客户端重试算法仿真 (`CLIENT_SPEC.md` §3)**：
   - 仿真实现客户端跳转探测循环（遍历 `jumpCode 1..255`，附加后缀并计算 `jumpFID` 发起 `Put`）。
   - 验证客户端在遇到槽位碰撞后能平滑探测出新槽位并完成别名绑定，随后通过逻辑 FID 或目标 FID 均可正常读回。

## 自动化测试验证

运行命令：
```sh
go test -race -count=1 ./...
go vet ./...
gofmt -l .
git diff --check
```

测试覆盖明细：
- `internal/daemon/alias_test.go`:
  - `TestOneHopAliasEndToEnd`: 别名端到端写入、去后缀读回、Head 元数据、范围读取、两 FID 物理同盘断言全部通过。
  - `TestOneHopAliasRejections`: `alias_for == fid`、长度小于 2、覆盖已有对象、对已有指针/目标做链式别名等非法尝试全部被严格拒绝。
  - `TestOneHopAliasClientJumpRetryAlgorithm`: 客户端逐一探测 jumpCode 重试算法全流程仿真通过。
  - `TestOneHopAliasTrimImmunity`: 105 个普通对象触发 Trim 压实，别名指针及目标均未被打包，整理后读回与冷启动重新加载读回均 100% 匹配。
- `internal/daemon/foundation_test.go`:
  - `TestRPCCommitReleasesPending`: 别名提交后释放全局网关预留。
  - `TestReplayRejectsDanglingAlias`: 拒绝索引尾部悬空孤立指示项。
  - `TestRestartReplaysAliasAndOccupancy`: 服务端重启后重放别名及磁盘空间占用。
  - `TestAliasPairPublishedTogether`: 索引同步与两项原子发布。
  - 并发测试：普通写入与别名写入在不同胜出顺序下的竞态竞争。
- `scripts/vps-smoke`:
  - 真实两盘 VPS 环境（常规盘 + zram 盘）下针对 32KB 别名对象执行完整 seed、flush、sysfs 彻底 reset 破坏性冷启动，以及 verify 读回比对，全流程通过。

## 里程碑 6 验收结论

- 一跳别名（`alias_for`）机制满足规范要求，同盘物理共置、去后缀读回、防覆盖、防链式、Trim 免疫以及全池网关竞争处理全部闭环。
- **里程碑 6 验收正式闭环。**
