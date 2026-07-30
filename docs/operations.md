# EasyBackup 运维手册

本文说明 EasyBackup 的安装、启动、S3 配置、备份恢复、巡检、保留策略、故障排查和安全边界。系统采用“每个快照多个独立分卷”的对象格式；归档完整性以客户端计算并写入 Manifest 的 SHA-256 为准，S3 ETag 仅作为对象元数据使用。

## 1. 运行前提

- Python 3.10 或更高版本。
- 进行 zstd 备份或大文件差分时需要 `zstd`；大文件差分还需要 64 位 `xdelta3`。tar 由 Python 标准库安全写入器生成，不需要外部 `tar` 或 `7z`。
- 使用系统托盘时需要安装 `tray` 可选依赖，并且主机必须有图形会话。
- 使用 S3 时需要能够通过 HTTPS 访问目标 Bucket 或 S3-compatible endpoint。
- 源目录、数据目录和本地备份目录需要具有足够的读写权限。

可在 PowerShell 中检查 Python、zstd 和 xdelta3：

```powershell
Get-Command python
Get-Command zstd
Get-Command xdelta3
python --version
zstd --version
xdelta3 -V
```

Linux/macOS 可使用对应系统的软件包管理器安装 `zstd` 和 `xdelta3`。普通分卷不生成大型临时归档；大文件差分为了不把可变源路径直接交给外部程序，会使用临时空间保存经 SHA-256 验证的当前版本、Patch 和一次验证重建。最坏临时空间应按“Base + 当前文件 + Patch + 重建文件”预留。

## 2. 从源码安装

在仓库根目录创建独立虚拟环境：

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .
```

需要托盘功能时：

```powershell
python -m pip install -e ".[tray]"
```

开发和测试环境可以安装：

```powershell
python -m pip install -e ".[dev,tray]"
pytest
```

## 3. 配置与数据目录

EasyBackup 从环境变量读取进程级配置。任务路径、Cron、存储目标和快照索引保存在 SQLite；S3 密钥保存在系统 Keyring 或显式选择的加密文件后端中。

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `EASYBACKUP_DATA_DIR` | Windows 为 `%LOCALAPPDATA%\EasyBackup`；其他平台使用 `$XDG_DATA_HOME/easybackup` 或 `~/.easybackup` | SQLite、锁、日志和凭据元数据目录 |
| `EASYBACKUP_HOST` | `127.0.0.1` | FastAPI 监听地址 |
| `EASYBACKUP_PORT` | `8765` | FastAPI 监听端口 |
| `EASYBACKUP_TIMEZONE` | `Asia/Shanghai` | 未单独指定时的调度时区 |
| `EASYBACKUP_SCRUB_SCHEDULE` | `0 3 * * sun` | 对每个已启用任务的最新快照执行抽样巡检；设为空字符串可禁用 |
| `EASYBACKUP_ALLOWED_HOSTS` | 空 | 额外允许的 HTTP Host，多个值用逗号分隔；非通配监听时默认包含监听主机和 loopback |
| `EASYBACKUP_LOG_LEVEL` | `INFO` | 日志级别 |
| `EASYBACKUP_API_TOKEN` | 未设置 | 非 loopback 绑定时强制要求 |
| `EASYBACKUP_CREDENTIAL_BACKEND` | `auto` | `auto`、`keyring` 或 `encrypted_file` |
| `EASYBACKUP_OPEN_BROWSER` | `true` | 启动后是否打开本地控制台 |
| `EASYBACKUP_SHUTDOWN_TIMEOUT` | `30` | 优雅退出最多等待秒数 |
| `EASYBACKUP_RECONCILE_INTERVAL` | `60` | 对因租约或存储故障延后的 RUNNING 快照再次对账的间隔秒数 |
| `EASYBACKUP_INTEGRITY_BLOCK_SIZE` | `8388608` | 分块 CRC32 的块大小，单位字节 |
| `EASYBACKUP_XDELTA3_PATH` | 自动查找 | 可选的 xdelta3 绝对路径；Windows 同时搜索活动/基础 Conda 的 `Library\bin` |

数据目录的典型内容：

```text
EasyBackup/
├── easybackup.db
├── locks/
├── logs/
├── delta-cache/           # 可从远端 Base 重建的明文性能缓存
└── secrets/
```

不要把数据目录放在任何任务的源目录内，否则数据库、日志和锁文件会持续变化并进入备份。数据目录本身需要通过独立方式保护；远端 Manifest 可以帮助恢复快照索引，但不能代替安全保存凭据。

## 4. 启动方式

前台运行服务：

```powershell
easybackup serve
```

也可以通过模块入口启动：

```powershell
python -m easybackup serve
```

默认控制台地址：

```text
http://127.0.0.1:8765/
```

启动系统托盘：

```powershell
easybackup-tray
```

托盘仅是本地控制入口，不等于已经注册为开机启动或系统服务。需要无人值守运行时，应使用操作系统服务管理器或任务计划程序，并确保：

- 服务账户可以读取所有源目录；
- 服务账户能访问配置所用的 Keyring；
- 采用 zstd 的任务需确保 `zstd` 位于该服务账户的 `PATH`；
- `EASYBACKUP_DATA_DIR` 指向固定且可写的目录；
- 工作目录不会影响源路径解析；
- 停止服务时会先发送正常终止信号。

使用 `Ctrl+C` 或正常服务停止流程退出。退出时系统停止接收新操作，在配置的超时预算内等待或协作取消当前分块、刷新 SQLite 状态，并对超时的 zstd 压缩/解压子进程执行 terminate/kill 清理。不要绕过服务直接终止 zstd。极端情况下，操作系统或对象存储 SDK 中不可中断的阻塞 I/O 仍可能超过应用层超时；无人值守部署应同时配置服务管理器的最终停止超时。

## 5. S3 配置

### 5.1 Bucket 和权限

建议为 EasyBackup 使用独立 Bucket，或至少使用独占前缀。运行身份通常需要以下能力：

- 列举目标前缀对象；
- 读取、写入和删除对象；
- 创建、列举、完成和中止 Multipart Upload；
- 对租约对象执行条件写入；
- 读取对象元数据和 Range；
- 可选：管理限定在 EasyBackup 前缀的未完成 Multipart 生命周期规则。

AWS IAM 动作通常涉及：

```text
s3:ListBucket
s3:GetObject
s3:PutObject
s3:DeleteObject
s3:ListBucketMultipartUploads
s3:ListMultipartUploadParts
s3:AbortMultipartUpload
```

权限应限定到实际 Bucket 和前缀。不要为了排错长期授予账户级或全 Bucket 管理权限。

### 5.2 凭据

先在 Web UI 或凭据 API 中创建凭据 Profile，再在任务的 S3 设置中引用其名称。凭据包含：

- Access Key ID；
- Secret Access Key；
- 可选 Session Token。

默认 Profile 名称为 `default`。读取凭据状态时只会返回 Access Key 提示和后端类型，不会返回 Secret。

推荐使用 `keyring`。无人值守账户无法使用系统 Keyring 时，可以显式设置：

```powershell
$env:EASYBACKUP_CREDENTIAL_BACKEND = "encrypted_file"
```

该模式使用机器标识派生密钥进行 AES-256-GCM 落盘，只能降低凭据被偶然明文读取的风险，不能防御拥有本机高权限的攻击者。机器标识变化后旧凭据可能无法解密，应重新录入凭据。

### 5.3 任务中的 S3 参数

S3 任务配置示例：

```json
{
  "name": "documents-nightly",
  "source_path": "D:\\Data\\Documents",
  "storage": {
    "kind": "s3",
    "bucket": "company-backups",
    "prefix": "easybackup/workstation-01",
    "region": "ap-southeast-1",
    "endpoint_url": null,
    "credential_profile": "production",
    "storage_class": null,
    "multipart_chunk_mb": 16
  },
  "schedule": "0 2 * * *",
  "compression": "zstd",
  "compression_level": 3,
  "shard_size_mb": 256,
  "full_every": 6,
  "retention_chains": 3,
  "retention_days": 30,
  "follow_symlinks": false,
  "delta_enabled": true,
  "delta_threshold_mb": 100,
  "delta_max_ratio": 0.9
}
```

S3-compatible 服务可设置 `endpoint_url`。生产环境应使用受信任证书的 HTTPS endpoint。若 Provider 不支持 `If-None-Match` 和 `If-Match` 条件写入，远端租约无法提供可靠的多主机并发排他；此时不要让多台 EasyBackup 实例写入同一个 Bucket 前缀。即使 Provider 支持条件写，TTL 租约也不是严格 fencing-token 协议；同一仓库必须由一个权威 SQLite catalog/写入主机管理，不能让拥有独立数据库的多台实例轮流写同一前缀。

租约心跳异常会 fail-closed，写入 Commit 和整链清理前还会同步条件续租。
但基于 TTL 的对象租约不是服务端严格 fencing 事务：多台主机必须保持时钟同步，
也不应在会挂起进程超过租约 TTL 的环境中主动配置多写者。需要严格多主写入时，
应在上层增加带单调 fencing token 的协调服务。

### 5.4 远端对象布局

每个快照包含多个不可变分卷：

```text
{prefix}/v1/tasks/{task_id}/
├── write.lock.json
└── chains/{chain_id}/snapshots/{snapshot_id}/
    ├── volumes/
    │   ├── {snapshot_id}-volume-00001.tar.zst
    │   ├── {snapshot_id}-volume-00002.tar.zst
    │   └── ...
    ├── patches/
    │   └── {snapshot_id}-delta-00001.vcdiff.zst
    ├── manifest.json
    └── commit.json
```

发布顺序是分卷、Manifest、Commit。只有存在有效 `commit.json` 的快照才可用于恢复、巡检和保留策略。不要手工覆盖任何已提交对象。

建议在 Bucket 上启用：

- 默认服务端加密，例如 SSE-S3 或 SSE-KMS；
- Bucket versioning 或 Object Lock，若业务需要防误删或勒索保护；
- 仅针对 EasyBackup 前缀的未完成 Multipart Upload 7 天清理；
- 阻止公开访问；
- 访问日志和异常访问告警。

## 6. 归档工具和压缩

任务的 `compression` 可设置为：

- `auto`：优先选择可用的高效压缩方式；
- `zstd`：使用外部 zstd；
- `gzip`：兼容性后端；
- `none`：仅打包，不压缩。

zstd 默认压缩等级为 3，允许范围为 1 至 19。较高等级通常降低上传量，但会增加 CPU 使用和运行时间。不要只根据压缩率调高等级，应结合源数据类型、备份窗口和网络带宽测试。

gzip 使用同一个配置字段，但 Python gzip 后端支持 1 至 9；若任务填写 10 至 19，会安全限制为 9。`none` 模式忽略压缩等级。

`shard_size_mb` 是规划分卷时的源数据目标值，不代表最终对象严格等于该大小：

- 默认 256 MiB；
- 单个大文件可独占并超过目标值；
- 压缩后的对象大小取决于内容；
- 选择性恢复只下载包含所选文件的分卷；
- 分卷边界也是中断后可安全重做的边界。

当 Python tar 写入器发现源文件身份、元数据或实际归档字节摘要变化时，运行会失败并清理未发布对象；不会写入 Commit。zstd 失败时检查其退出码和 stderr。正常停止服务会回收管道与 zstd 子进程。

### 6.1 大文件 Base-relative 差分

启用 `delta_enabled` 后，内容发生变化且不小于 `delta_threshold_mb` 的文件进入差分候选。算法固定为：

```text
本链最早完整 Base + 当前版本 ──xdelta3──► VCDIFF Patch ──zstd──► 远端对象
```

每个 Patch 都直接引用本链该路径最早的完整版本，不引用前一天 Patch。默认每日任务在 1 次全量后运行 6 次增量，第 8 次运行建立新全量链，所以任意版本恢复最多应用一个 Patch。

提交 Patch 前系统会：

1. 从经过大小、时间、文件身份和 SHA-256 校验的暂存副本读取当前版本；
2. 校验远端 Base 对象仍存在且大小正确；缓存未命中时从远端完整分卷物化并核对 Base SHA-256；
3. 生成 Patch，并实际执行一次 `Base + Patch` 重建；
4. 核对重建文件的大小和 SHA-256；
5. zstd 压缩。若压缩后大小达到 `原文件大小 × delta_max_ratio`，则丢弃 Patch 并回退完整分卷。

缺少 xdelta3/zstd、Patch 生成失败、Base 不可用或 Patch 不划算都只会使该文件回退完整归档，不会发布不完整差分。恢复已提交的差分文件时缺少 xdelta3 或 zstd 都不能无损回退，必须先安装工具。

生成与还原期间会轮询外部进程的输出大小。裸 VCDIFF Patch 最多允许“目标文件大小 + 64 MiB”，重建输出不得超过 Manifest 声明的目标大小；越界时立即终止子进程并清理未发布临时文件。

## 7. 备份与调度

首次成功备份为全量快照。之后通过 `mtime_ns + size + type` 快速比较，元数据变化的文件再计算 SHA-256。新增和修改文件进入新分卷，删除路径写入 Manifest tombstone。

修改任务的源根目录会清空该任务的快速扫描状态，并根据 Manifest 中不泄露路径明文的 source fingerprint 强制创建新的全量链；不会复用旧目录中碰巧具有相同相对路径、大小和时间戳的内容。

默认策略：

- 每 6 次成功增量后创建新的全量链；每日任务对应“周一全量、周二至周日增量”；
- 始终保留最近至少 3 条链；
- 对更旧的链，仅当整链最新快照也早于 30 天期限时才删除；
- 软链接默认不跟随；
- 默认排除 `.git/**`、`__pycache__/**`、`*.tmp`、`*.part`、`Thumbs.db` 和 `.DS_Store`。

`schedule` 使用五字段 Cron 表达式：

```text
minute hour day-of-month month day-of-week
```

例如每天 02:00：

```text
0 2 * * *
```

在正式启用定时任务前，应先手动运行一次并完成恢复演练。备份成功只代表对象和 Manifest 已提交，不代表未经演练即可恢复业务。

## 8. 恢复

恢复可以选择整个快照、目录或文件。普通条目根据 Manifest 的 file-to-volume 映射只读取相关分卷；差分条目只读取完整 Base 分卷和当天 Patch，应用一次 xdelta3。Base、Patch 远端对象和最终重建文件均进行完整性校验，最终文件使用同目录临时文件成功后，以原子覆盖或原子“不覆盖”方式发布。每个差分文件使用独立临时目录并在完成后立即释放；容量规划仍应为单个最大文件预留“压缩 Base 对象 + 解压 Base + 压缩/裸 Patch + 重建输出”的峰值空间。

恢复前：

1. 确认快照状态为已提交且未标记损坏。
2. 选择与源目录不同的空目标目录做演练。
3. 确认目标盘可用空间。
4. 选择覆盖策略。

覆盖策略：

- `skip`：默认值；保留现有文件，即使下载期间有其他程序创建同名文件也不会覆盖；
- `overwrite`：替换现有文件；
- `rename`：通过原子不覆盖提交选择不冲突名称。

系统应拒绝绝对归档路径、`..` 路径穿越和逃出目标根目录的链接。不要为了恢复未知来源的手工归档而关闭这些检查。

从增量快照恢复时，Manifest 中的条目可能引用同一链较早快照的分卷。大文件 Patch 直接引用周期 Base，中间 Patch 不属于后续版本依赖；但保留策略仍以整链为单位，不能手工删除 Base 或任一 Manifest/Commit。恢复完成后应检查操作状态和校验结果，而不应仅以目标目录中出现文件作为成功依据。

## 9. 巡检

默认巡检请求使用 1% 抽样，至少应检查一个对象。抽样校验使用 Manifest 中的客户端校验信息；深度巡检读取完整对象并重新计算 SHA-256。

完整性规则：

- 客户端 SHA-256 是归档对象的权威摘要；
- CRC32 块索引用于低成本抽样和定位损坏范围；
- `Content-Length` 用于快速发现截断；
- S3 ETag 不作为 MD5 或完整性证明；
- 巡检失败把快照的 `verify_status` 标记为 `missing` 或 `corrupt`，但不会自动删除唯一备份。

建议：

- 每周执行一次 1% 抽样；
- 每月或每季度执行一次深度巡检，频率取决于数据量和成本；
- 每次存储迁移、密钥策略变更或 Provider 故障后执行深度巡检；
- 对失败对象保留日志、对象键、期望和实际 SHA-256，再决定从其他副本修复。

## 10. 保留与 Multipart 清理

保留策略以完整链为删除单位。一条链由一个全量快照和其后所有增量快照组成。系统不得留下依赖已删除全量快照的悬挂增量。

清理前确认：

- 最近至少有一条经过恢复演练的成功链；
- 当前没有备份、恢复或巡检正在使用目标链；
- Bucket versioning、Object Lock 和生命周期规则不会使预期成本失控；
- 当前凭据具有精确的对象删除权限。

清理以先移除 Commit、再删除 Manifest/分卷/Patch、最后删除 SQLite 记录的顺序执行，并移除该链可重建的本地 Base 缓存。对象删除是幂等的；中断时数据库记录会保留，再次执行会继续同一条链的清理。

S3 Multipart Upload 在失败进程退出后可能保留未完成分片。建议配置 7 天生命周期清理，并定期运行应用级 stale multipart cleanup。生命周期规则必须限定 EasyBackup 前缀，避免清理同 Bucket 中其他应用的上传。

## 11. 日志和监控

日志位于：

```text
{EASYBACKUP_DATA_DIR}/logs/
```

排查时优先记录：

- task、snapshot 和 operation ID；
- 当前阶段：scan、diff、pack、compress、upload、commit、restore、scrub 或 prune；
- 外部工具退出码；
- S3 HTTP 状态码和错误码；
- 期望与实际对象大小/SHA-256；
- 远端租约 owner 和过期时间，但不要记录 token。

日志脱敏是最后一道防线。不要在命令行、任务名称、Bucket prefix、异常消息或调试截图中主动放入 Access Key、Secret Key、Session Token 或 API Token。

## 12. 故障排查

### Web UI 无法打开

- 确认服务进程仍在运行。
- 检查 `EASYBACKUP_HOST` 和 `EASYBACKUP_PORT`。
- 检查端口是否已被其他程序占用。
- 使用 `http://127.0.0.1:8765/`，不要把默认 loopback 地址替换成局域网地址后仍省略 Token。
- 查看日志中的启动和数据库迁移错误。

### 非本机监听启动失败

设置高熵 API Token：

```powershell
$env:EASYBACKUP_HOST = "0.0.0.0"
$env:EASYBACKUP_API_TOKEN = "<a-long-random-secret>"
easybackup serve
```

浏览器首次访问时会显示 Token 验证对话框。Token 只通过同源 `POST`
请求换取 HttpOnly、SameSite 会话 Cookie，不会写入 `localStorage`、
`sessionStorage` 或 WebSocket URL。非浏览器 REST 客户端使用：

```http
Authorization: Bearer <a-long-random-secret>
```

WebSocket 客户端可发送同一 `Authorization` 请求头；浏览器会自动使用
上面的会话 Cookie。不要把 Token 放在查询参数中，因为代理和访问日志可能
记录完整 URL。

内置服务不应直接暴露到公网。跨主机访问还需要可信反向代理、TLS、来源限制和正式身份认证。

### 找不到 zstd

- 使用 `Get-Command zstd` 检查服务账户的 PATH。
- 交互用户安装成功不代表 Windows 服务账户可见。
- 明确选择 `gzip` 或 `none` 可用于诊断，但不能掩盖配置为 `zstd` 时工具缺失的问题。
- 安装工具后重启 EasyBackup 服务，使其重新读取 PATH。

### 找不到 xdelta3

- 运行 `easybackup doctor`，确认 `tools.xdelta3.available` 和检测到的路径。
- Windows + Miniconda 默认搜索 `Library\bin\xdelta3.exe`；也可设置 `EASYBACKUP_XDELTA3_PATH` 为绝对路径。
- 新备份会对候选大文件回退完整分卷；已有差分版本的恢复必须有 xdelta3，不能用 Base 冒充目标版本。
- 不要从不明来源在运行时自动下载可执行文件。安装后核对发布方 SHA-256，并重启服务。

### S3 返回 403

- 验证凭据 Profile 名称与任务配置一致。
- 检查 Session Token 是否过期。
- 检查 Bucket、region、endpoint 和前缀。
- 分别确认 ListBucket、对象读写、删除和 Multipart 权限。
- 使用 SSE-KMS 时确认 KMS key 权限。
- 不要把 403 简单归因于网络问题后改用明文 HTTP。

### S3-compatible Provider 出现 409 或 412

这些状态通常与条件租约写入竞争有关：

- 查看是否有另一实例正在运行相同任务。
- 检查远端租约的 owner、heartbeat 和 expires_at。
- 等待租约自然过期后重试。
- 确认主机时间同步。
- Provider 若不支持条件 PUT，不要并行运行多主机写入。

只有确认所有写入实例均已停止、租约已过期且对象键完全正确时，才考虑手工处理租约对象。不要批量删除整个 `locks/` 或快照前缀。

### 操作长期停在 uploading

- 检查网络吞吐和 S3 限流。
- 检查 EasyBackup 进程及 zstd/xdelta3 子进程是否仍有 CPU 或 I/O。
- 检查目标 Bucket 是否出现未完成 Multipart Upload。
- 检查源文件是否持续变化或被安全软件锁定。
- 正常取消操作；不要同时强杀 FastAPI、zstd 和 xdelta3。

### SHA-256 不一致

- 停止对受影响链执行保留清理。
- 对该对象执行深度巡检并记录对象键。
- 检查对象大小和 Manifest 中的预期摘要。
- 从 Bucket versioning、Object Lock 副本或另一备份恢复。
- 不要用 S3 ETag 覆盖 Manifest 中的 SHA-256。
- 修复前将该链视为不可独立恢复。

### Keyring 不可用

- 确认当前账户有可用的系统 Keyring 后端和交互/服务权限。
- 若强制使用 `keyring`，不可用时服务应失败而不是静默降级。
- 只有接受机器绑定后端的安全限制时才选择 `encrypted_file`。
- 机器迁移或 MachineGuid 变化后重新录入 Profile。

### SQLite 报 locked 或损坏

- 确认只有一个 EasyBackup 数据目录实例在作为主进程运行。
- 检查数据目录是否位于不可靠的网络共享或同步盘。
- 正常停止所有实例后再备份数据库文件并诊断。
- 不要在运行中复制 WAL 数据库的单个主文件作为一致备份。
- 启动与周期对账只处理 SQLite 中仍为 `RUNNING` 的记录：有有效 Commit 时补完本地提交；确认 Commit 缺失时，在取得本机锁和远端租约后清理该快照的未发布前缀。Commit 已存在但其他元数据暂不可见或无法验证时会保留远端证据并延后重试。
- 对账不会在数据库完全丢失后枚举并导入全部远端历史，也无法发现“没有对应 RUNNING 行”的任意孤儿对象。Bucket versioning、Object Lock 旧版本和未完成 Multipart Upload 仍分别由 Provider 生命周期与 Multipart 清理功能管理。应独立保护 `easybackup.db`、任务配置和凭据。
- 只有 FAILED 记录的任务可删除；包含 RUNNING 或 COMPLETED 快照时会拒绝删除，以免丢失本地恢复索引。

### 恢复路径被拒绝

- 路径必须来自该快照 Manifest。
- 不允许绝对路径、`..`、目标根目录外链接或平台非法名称。
- Windows 大小写碰撞、保留名和路径长度也可能导致拒绝。
- 不要关闭安全校验；选择新的空目标目录并解决名称冲突。

## 13. 安全与一致性边界

必须理解以下边界：

- 默认只监听 loopback；API Token 不能替代公网级身份认证和 TLS。
- 备份分卷当前是压缩数据，不等于客户端加密。应依赖私有 Bucket、TLS 和 Provider 端 SSE；高度敏感数据需要额外的客户端加密层。
- SHA-256 检测意外损坏，但没有密钥的普通摘要不能单独防止有权限同时替换对象和 Manifest 的攻击者。
- `encrypted_file` 凭据后端的机器标识不是秘密，不能抵御本机管理员或同等权限攻击者。
- 没有 VSS、LVM/ZFS snapshot 或应用原生快照时，正在变化的数据库和业务文件不具备应用一致性。应先调用应用自身备份/冻结机制，再让 EasyBackup 备份其稳定输出。
- `delta-cache` 是可删除的明文性能层，不是恢复依据；高敏感源数据应把整个数据目录放在受控磁盘/账户边界内。远端 Manifest 自包含 Base 定位，缓存丢失后仍可恢复。
- `mtime + size` 快速比较可能漏掉保留时间戳和大小的内容改动；定期深度巡检和周期性全量链用于降低风险。
- 不跟随软链接是默认安全行为。开启后只固定并读取扫描时仍位于源目录内的 canonical target，跨源链接会跳过、目录循环会检测；它仍不等同于 VSS、文件系统快照或应用一致性机制。
- 保留策略不是灾难恢复策略。至少保留一份独立账户、独立权限域或离线副本。
- 成功上传不等于可恢复。必须周期性执行隔离目录恢复演练。

## 14. 建议运行节奏

- 每日：查看失败操作、租约冲突和凭据过期。
- 每周：运行 1% 抽样巡检，检查未完成 Multipart Upload。
- 每月：验证保留链数量，执行一次隔离目录恢复演练。
- 每季度：执行深度巡检，检查 Bucket 权限、加密、versioning 和生命周期规则。
- 每次升级后：创建测试任务，完成 full、incremental、单文件恢复、巡检和保留清理闭环。
