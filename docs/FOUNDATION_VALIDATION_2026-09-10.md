# 基础修补与验证（2026-09-10）

范围：在继续里程碑 4 前加固里程碑 1–3 的基础实现，并检验已有多盘
改动所依赖的并发语义。保留原有工作区改动；本轮未提交、未写入 USB 盘。

## 已复现并修复

新增测试在修改业务实现前确认以下四项失败：

- 成功 alias Put 后 pool gate 残留 2 个 pending FID。
- 5 MiB Get 被默认 gRPC 客户端以超过 4 MiB 消息限制拒绝。
- 数据区被截短后，Get 将短读后的零填充内容作为成功结果返回。
- 索引尾部只有 jump indicator 时，OpenDevice 仍然成功。

对应修复：所有 Put 退出路径释放 pool pending；Get 使用最多 1 MiB
的按范围磁盘读取与流式响应；磁盘短读返回错误；启动重放严格检查索引，
对不完整 alias、重复 FID、重叠已确认区间和读取错误明确拒绝启动。

## 同时完成的基础加固

- 每盘启动重建 confirmed 内存索引及索引尾部位置；Head/Confirmed 不再
  分配 4 MiB 缓冲扫描磁盘。alias 与目标只在完整提交后一起发布。
- 提交顺序为 payload Sync → 完整索引记录/索引对 WriteAt → index Sync
  → 内存发布。索引写入或同步失败时清零该次追加区域并同步。
  清理失败则将设备标记故障，拒绝继续预留及查询，Health 返回 false。
- 短写也被视为错误，不将部分写入当成完整提交。
- OpenDevice 检查 NBSS v2 header、disk ID 和有效数据区大小。
  NewServer 启动中途失败会关闭已经打开的设备，并拒绝重复 disk ID
  或跨盘重复的已确认 FID。
- 排队获取 I/O 配额支持取消；Put 排队等待受停滞超时约束。
  空 chunk 不重置“收到有效字节”的期限；提交前检查请求是否已取消。
- 旧并发测试用 channel 确认预留到位，移除 20ms Sleep 顺序假设；
  修正 alias 测试 payload 缺少尾部跳转字节的问题。
- 小写入与槽尾补零缓冲按实际长度分配，避免每次都分配两块 4 MiB 缓冲。

## 验证覆盖

`internal/daemon/foundation_test.go` 使用具有合法 header 的临时稀疏盘，
两盘测试采用不同 disk ID。传输测试使用真实 gRPC 客户端和 bufconn；
需要精确暂停预留或提交时使用可控流/文件接口，直接执行生产编排逻辑。

- 普通与 alias 写入、成功后的重复写冲突及 pending 清空。
- 5 MiB 对象使用默认客户端完整读回；范围末端、空范围和非法范围。
- 短流、超长流、客户端取消、停滞、持续空 chunk、配额等待取消/超时。
- plain/alias 竞争的两种胜者顺序；竞争请求大小使其只能落到第二块盘，
  但被全池 gate 提前拒绝；pending Head 不可见，胜者内容可读回。
- 64 路不同对象并发 RPC Put/Get；完成后没有累积 pending。
- 响应发送失败时已提交对象仍存在，pending 已回收。
- alias 对写到一半时，Head/Confirmed 不会观察到半提交。
- payload Sync 失败、索引对部分写入、index Sync 失败、回滚失败。
  回滚成功后重新打开磁盘确认对象不可见，并确认原 FID 可以重试；
  回滚失败后确认设备拒绝新预留。
- 重新打开磁盘后 alias、目标内容与已确认 extent 正确恢复。
- 正式 NewServer 从 pan.json 重建两盘，并发现仅存在于第二盘的对象。
- header 魔数、版本、身份、容量检查；索引读取故障；孤立 alias。
- 第二盘配置解析/打开失败后，通过 /proc/self/fd 确认第一盘无句柄泄漏。

## 尚未声称完成的边界

1. 真正掉电或内核崩溃下的扇区撕裂，不能由 Go race 或文件接口故障注入
   证明。现有 NBSS 索引格式没有新增 WAL/事务标记；重启发现不完整 alias
   会拒绝启动，不会擅自丢弃记录。成功响应要求 Sync 返回成功，但依赖
   系统和设备履行同步语义。尚未做物理断电或进程 kill 测试。
2. 目前采用 buffered I/O + Sync，尚未实现设计中的 O_DIRECT。
   每次提交同步的真实设备吞吐与延迟尚需测量；不再将其描述为已经具备
   原 NBSS 的 O_DIRECT 写路径。

   > **已解决（2026-09-11）**：O_DIRECT 已接入主写入路径并在授权测试 VPS 上
   > 完成真实验证（日志 + `/proc/<pid>/fdinfo` flags 位独立确认），见
   > [`docs/REMOTE_E2E_VALIDATION_ODIRECT_2026-09-11.md`](REMOTE_E2E_VALIDATION_ODIRECT_2026-09-11.md)。
   > 上面这段原文按当时状态如实保留，不做改写。
3. zram 冷启动发现/准备设备、快照恢复及默认目录对齐仍属于进行中的
   里程碑 4；本轮没有用普通文件测试替代真实 zram 验收。

   > **已解决（milestone 4，见 `docs/MILESTONE_4_VALIDATION_2026-09-10.md`）**：
   > 已在真实 zram 主机（授权测试 VPS）上完成门禁验收，覆盖冷启动自动发现、
   > 压缩快照恢复、sysfs 破坏性 reset 后的自动重载，SHA-256 全量校验通过。
4. Packed/Trim 读取和重放还未接入 daemon；启动对其明确报不支持，
   不能把“磁盘字节格式相同”解释为已经能加载所有 NBSS 历史盘。

   > **已解决（milestone 5，见 `docs/MILESTONE_5_VALIDATION_2026-09-10.md`）**：
   > `replayIndex` 已支持识别打包锚点、加载解析打包表、恢复 `confirmed`/
   > `packed` 映射；Trim 强制连续运行并有专门的故障注入与冷启动重放测试
   > 覆盖。
5. CHD 路由边界、zram 分层的完整矩阵以及两块 USB 盘实测尚未完成；
   这些条件满足前，里程碑 4 继续保持 in progress。

   > **已解决（milestone 4，见 `docs/MILESTONE_4_VALIDATION_2026-09-10.md`）**：
   > CHD/路由测试覆盖最小可容纳盘、无可信容量回退、zram 小对象优先、
   > 阈值等号、大对象不进 zram-only 池、空池/满盘/pending-abort 等边界；
   > 两块真实 USB 盘（SanDisk 57.3G、PHILIPS 7.5G）联调通过。

这些限制不会被本轮通过的基础回归测试掩盖。后续先完成里程碑 4 的
剩余验收，再推进 Trim/Bonnie 和客户端库。

## 最终检查结果

- `go test -race ./... -count=1 -timeout=60s`：通过（blk 1.028s，daemon 1.942s）。
- `go build ./...`、`go vet ./...`、`git diff --check`：通过。
- `gofmt -l internal/daemon/*.go`：无输出。
- 三项关键并发测试（跨盘冲突、64 路 RPC、alias 成对发布）在
  `-race -count=10 -cpu=1,4` 下通过，共覆盖两种调度并行度。
- 新增 17 个顶层基础测试（另含表驱动子测试）；原有测试继续通过。
