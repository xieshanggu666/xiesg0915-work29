# 机房设备退役与数据擦除管理系统

设备退役登记 → 拆盘 → 按标准全盘覆写 → 字节级复验 → 生成擦除证明 → 处置签收的全流程系统。

- **擦除任务与复验**：Go 实现，多道标准覆写（零 / 一 / 可重现随机流），全盘或抽样复验；多台设备并发擦除；每个 chunk 落断点并 fsync，断电/杀进程后重启从 `(道次, 偏移)` 续做。
- **资产与证明**：PostgreSQL 存储；资产状态机、任务、断点、执行记录、擦除证明、处置签收、审计日志。审计表/执行记录/证明/签收为**只追加**（数据库触发器禁止 UPDATE/DELETE）。
- **报告文件**：MinIO（S3 协议，内置手写 SigV4 客户端，零云 SDK 依赖）；未配置 MinIO 时自动落本地文件系统。每盘生成机器可读 JSON 报告 + 可打印 HTML《数据擦除证明》。
- **审计**：每台设备可拉出完整时间线——谁、什么时间、做了什么。
- **处置签收不可覆盖**：一台设备只有一行签收（UNIQUE + 只追加触发器），重复签收直接 409。

## 状态机

```
资产  in_service → pending(退役登记) → disk_pulled(拆盘) → erasing(擦除中)
                         └──────────────┴──→ scrapped(报废) ──→ disposed(处置,终态)
                                     erasing → erasure_verified(复验通过) ──→ disposed

磁盘  pulled(待擦) → erasing → verified(复验通过)
                    └→ failed(复验失败) → 重擦(新任务,attempt+1) / scrapped(报废)

任务  pending → running ⇄ (断电孤儿, 被重新领取) → verifying → verified / failed
```

校验失败的磁盘进入 `failed`，由操作人决策：
- `POST /disks/{id}/jobs` **重擦**（attempt + 1，每次成功各签一份证明）
- `POST /disks/{id}/scrap` **报废**（物理销毁，资产走 scrapped 分支）

## 快速开始（演示模式，无需 Postgres/MinIO）

```bash
go run ./cmd/server         # 内存存储 + 本地报告目录
DEMO=true go run ./cmd/server   # 附带一台已拆盘的演示设备（磁盘为镜像文件）
# 另一终端：
curl -s localhost:8080/api/v1/standards
curl -s -X POST -H 'X-Operator: alice' -H 'Content-Type: application/json' \
  -d '{"standard":"nist_purge"}' localhost:8080/api/v1/disks/<diskID>/jobs
curl -s localhost:8080/api/v1/assets/<assetID>/timeline
```

所有写接口必须带操作人：请求头 `X-Operator: <工号>`（或 `?operator=`）。

CLI 端到端演示（含断电续做）：

```bash
# 正常擦除（DoD 三道）
go run ./cmd/cli demo --wipe --standard dod_3pass

# 在第 100MiB 模拟断电（worker 被直接杀死，不清理），随后"重启"新进程从断点续做
go run ./cmd/cli demo --wipe --standard nist_clear --crash-at-mb 100
```

生成的报告位于 `.demo/reports/erasure-reports/reports/`，JSON 中的 `runs` 会同时记录
`interrupted`（中断时已写字节）与 `verified resumed=true`（续做完成）两条执行记录。

## 生产部署（PostgreSQL + MinIO）

```bash
docker compose up -d --build      # 起 postgres/minio,自动执行迁移,起 server
# 或手动：
psql "$DATABASE_URL" -f migrations/0001_init.sql
go run ./cmd/cli migrate --database-url "$DATABASE_URL"
DATABASE_URL='postgres://...' MINIO_ENDPOINT=localhost:9000 \
  MINIO_ACCESS_KEY=... MINIO_SECRET_KEY=... go run ./cmd/server
```

环境变量见 `.env.example`。安全护栏：`DEVICE_PATH_PREFIX`（默认 `/dev/`）限制可擦除的设备路径，
防止误擦系统盘；生产部署擦除节点时应只放行专用裸盘挂载点。

## 覆写标准

| code | 道次 | 复验 | 用途 |
|---|---|---|---|
| `nist_clear` | 0x00 ×1 | 全盘 | 组织内复用 |
| `nist_purge` | 0xFF / 随机 / 0x00 | 全盘 | 介质离开组织 |
| `dod_3pass` | 0x00 / 0xFF / 随机 | 全盘 | 常见合同要求 |
| `sample_quick` | 0x00 ×1 | 1% 抽样（首尾必抽） | 演练/低密级 |

随机道使用以 `(jobID, 道次)` 为密钥的 ChaCha8 密钥流，因此任意偏移在崩溃后可**独立重现**——
这是随机道也能精确断点续做、并逐字节复验的关键。对 SSD/NVMe 生产场景还应配合厂商 NVMe Sanitize/Crypto Erase（本系统负责可审计的覆写与证明链路）。

## 断点续做如何工作

1. 写入循环按 4 MiB 块写盘，每累计 `CHECKPOINT_EVERY_BYTES`（默认 64 MiB）执行 `fsync`，
   并在 `checkpoints` 表 upsert `(job, pass, offset, heartbeat)`。
2. 擦除中断电：进程消失，最后一个断点之前的数据已落盘。
3. 新进程启动时立即把**上一进程**（按 worker 实例 ID 区分）持有的 running/verifying 任务
   标记为可回收（`SKIP LOCKED` 原子领取，多实例安全），读取断点，`seek(offset)` 从当前道次继续；
   已完成的道次不重写。长期运行时则改由心跳超时判定僵死（默认 120s）。

## HTTP API 摘要

| 方法 & 路径 | 说明 |
|---|---|
| `GET /healthz` | 健康检查（含存储后端） |
| `GET /api/v1/standards` | 覆写标准列表 |
| `POST /api/v1/assets` | 退役登记（body: tag/vendor/model/sn/room/rack/owner…） |
| `GET /api/v1/assets` | 资产列表（?status=&tag=&limit=&offset=） |
| `GET /api/v1/assets/{id}` | 资产详情 |
| `POST /api/v1/assets/{id}/disks` | 拆盘登记 `{"disks":[{serial,model,kind,capacity_gb,device_path,slot}]}` |
| `GET /api/v1/assets/{id}/disks` | 设备下磁盘 |
| `GET /api/v1/assets/{id}/certificates` | 该设备的擦除证明 |
| `GET /api/v1/assets/{id}/timeline` | **设备审计时间线**（资产事件+每盘事件+任务） |
| `POST /api/v1/assets/{id}/disposal` | 处置签收（reuse/resale/destroy + receiver）；重复签收 409 |
| `GET /api/v1/assets/{id}/disposal` | 查签收 |
| `POST /api/v1/disks/{id}/jobs` | 创建擦除任务/重擦 `{"standard":"nist_clear"}` |
| `POST /api/v1/disks/{id}/reverify` | 只读独立复验（发现残留则盘置 failed） |
| `POST /api/v1/disks/{id}/scrap` | 报废 `{"reason":"..."}` |
| `GET /api/v1/disks/{id}/jobs` | 磁盘任务历史 |
| `GET /api/v1/jobs/{id}` / `.../runs` | 任务状态 / 每次执行记录（含 interrupted） |
| `GET /api/v1/certificates` | 证明列表（?asset_id=） |
| `GET /api/v1/certificates/{certNo}` | 证明元数据 |
| `GET /api/v1/reports/{certNo}` | 下载 JSON 报告 |
| `GET /api/v1/reports/{certNo}/certificate` | 下载 HTML 证明 |
| `GET /api/v1/audit` | 全局审计（?entity_type=&entity_id=&actor=） |

## 项目结构

```
cmd/server           HTTP API + worker 池（同一进程）
cmd/cli              migrate / demo（含断电模拟）
internal/domain      实体、状态机、覆写标准
internal/erasure     覆写引擎、逐字节复验、随机密钥流、断点
internal/store       Store 接口；memory(演示/测试) 与 postgres(pgx, SKIP LOCKED)
internal/worker      调度池、并发槽、断电回收、失败决策、签发证明
internal/report      JSON/HTML 报告与《数据擦除证明》
internal/objectstore MinIO/SigV4 与本地 FS 后端
internal/service     业务编排与生命周期校验
internal/api         HTTP/JSON 接口
migrations           PostgreSQL schema（状态机触发器 + 只追加触发器）
```

## 测试

```bash
go test ./... -race
```

覆盖：多道标准覆写与全盘复验、崩溃后断点续做（单道/三道中途断电）、篡改检出、
抽样复验、6 盘并发擦除、失败→重擦→两证、报废、证明/签收不可覆盖、
全流程 HTTP 集成、MinIO SigV4 签名（内置 S3 仿真服务端校验）。
