# 里程碑 4 验证进展（2026-09-10）

状态：路由与恢复实现完成；默认自动化及真实双盘测试通过；已在授权专用 Debian 13
VPS 上跑通真实 zram 门禁验收及服务冷启动自动恢复集成冒烟，里程碑 4 验收闭环。

## 本轮完成

- CHD/路由测试覆盖最小可容纳盘、无可信容量时回退、zram 小对象优先、
  阈值等号、大对象不进入 zram-only 池、空池、满盘、pending/abort/
  commit/reopen 对容量估计的影响。
- 复现并修复极端 small-file-pow2 造成负移位崩溃；修复数据区大小下溢、
  故障盘仍可选、非整槽容量误判，以及 NaN 概率未回退默认值。
- zram 按稳定 disk ID 发现快照和当前已加载设备，冷启动分配空设备后
  恢复；不再要求旧 pan.json 中的 zram 编号仍然存在。
- 默认快照目录统一为 `~/.bsos_zram_snapshots`。守护进程可用
  `-zram-snapshot-dir ''` 禁用恢复；普通自动化测试明确禁用主目录恢复。
- 校验快照 header/version/ID、长度及 SHA-256 后才分配设备。
  压缩快照要求元数据；旧 raw 快照无元数据时仍检查 header 和完整长度。
  raw/zstd 同时存在时优先 zstd，不重复恢复。
- 不重置旧配置路径上的已占用设备；加载失败会重置本次分配的空设备。
  缺失/损坏的 tier 显式记录错误，并让 Health.Ok 为 false。
- 恢复成功后原子更新 pan.json 的设备映射；暂时缺失的配置保留以便
  下次启动恢复。快照命令也校验实际设备 ID，避免旧路径造成错标快照。
- 修复移植代码对 hot_add 的调用：该 sysfs 属性通过读取来创建设备并
  返回编号，而不是写入。依据：
  https://docs.kernel.org/admin-guide/blockdev/zram.html#add-remove-zram-devices

## 自动化测试

新增路由测试、模拟内核恢复测试以及两个显式 opt-in 的真实设备门禁。
恢复测试覆盖发现未配置快照、旧设备编号失效、已加载设备不被重复恢复、
缺失/损坏/校验失败/恢复失败、普通盘身份冲突、恢复后配置持久化等。

普通命令：

```sh
go test -race ./... -count=1 -timeout=90s
go build ./...
go vet ./...
git diff --check
```

## 已执行：真实双 USB 盘

通过稳定 by-id 路径重新核对两块盘均为 NBSS v2、不同 disk ID、未挂载，
没有运行中的 bsosd/nbssd。本轮没有格式化、擦除或重置磁盘。

`TestRealMultiDisk` 分别用单盘池追加 1 MiB 和 2 MiB 测试对象，随后
打开组合池，验证两盘读回、全池重复 FID 拒绝、alias 写入、关闭后重新
打开及范围读回。测试于本机通过，总计约 3.23s。

- 1 MiB 写入（含 RPC、同步）：约 53.7ms。
- 2 MiB 写入（含 RPC、同步）：约 1.348s。

这只是小规模正确性实测，不是吞吐基准，也不声称两个空盘在相同小对象
负载下会自然均匀分配。均匀分配不属于 best-fit 策略的承诺。
写入的约 3 MiB 测试对象及小 alias 对保留在测试盘上（BSOS 无 DELETE）。
具体设备序列号、路径和测试对象 FID 未写入仓库。

复跑方式（会追加持久测试对象，仅用于已授权测试盘）：

```sh
BSOS_REAL_DEVICE_PATHS='["/dev/disk/by-id/<test-disk-1>","/dev/disk/by-id/<test-disk-2>"]' \
  go test ./internal/daemon -run '^TestRealMultiDisk$' -count=1 -v -timeout=180s
```

## 已执行：真实 zram 主机门禁（专用 Debian 13 VPS）

本机环境缺乏 zram 模块，已按授权在专用 Debian 13 VPS（Linux 6.12.38+deb13-cloud-amd64）
上执行物理门禁与端到端集成验证，全部通过：

1. **真实 zram 恢复门禁**：
   ```sh
   BSOS_REAL_ZRAM=1 go test ./internal/zram -run '^TestRealZramRecovery$' -count=1 -v
   ```
   耗时 4.20s，通过 raw 恢复、压缩快照生成、设备 reset、冷启动自动发现/重载，以及 SHA-256 完整读回校验。

2. **端到端 VPS 真实守护进程集成冒烟**：
   通过 `scripts/test-vps-setup.sh` 部署 384M 物理文件盘与 320M zram 设备，启动 systemd 管理的真实 `bsosd`：
   - 运行 `vps-smoke -mode seed`：小对象（64KB）及别名（32KB）自动路由至 zram，大对象（5MB）自动路由至常规盘，读回及范围读取 byte-exact 一致，短流截断自动释放且不可见，重复 FID 幂等拒绝；
   - 运行 `bsos zram flush`：生成持久化 zstd 压缩快照及 sha256 元数据；
   - 执行破坏性冷启动测试：停止服务，通过 sysfs 彻底 reset 清空 `/dev/zram0`；
   - 重启 `bsosd` 服务：守护进程自动发现快照，解压恢复入 zram 设备并上线；
   - 运行 `vps-smoke -mode verify`：全量对象读回校验、Head 元数据及 SHA-256 哈希全部 100% 匹配。

## 仍保留的边界

- 本轮没有做物理服务器突然掉电基准测试，快照生成期间调用方应确保存储内容稳定。
- Trim/Packed/Bonnie 和客户端库仍按后续里程碑推进。

## 本轮最终检查记录

- 非缓存全量 race：通过，blk 1.030s、daemon 2.022s、zram 6.769s。
- build、vet、diff 检查：通过；gofmt 检查无输出。
- 真实双盘门禁：通过（3.23s）。
- 真实 zram 门禁：通过（4.20s）。
- 真实 VPS 服务冒烟与冷恢复：通过。
- **里程碑 4 验收正式闭环。**
