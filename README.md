# DICOM 去标识化交付服务

影像数据工程师向研究库交付 DICOM 时使用的匿名化 API：上传含 **最多 20 份未压缩
DICOM** 的 ZIP，服务端替换患者标识、建立随机但**持久**的 UID 映射，处理完成后
下载匿名化 ZIP。基于 FastAPI + pydicom + SQLite，docker compose 启动
**两个独立 API 进程**共享同一映射库，支持同院跨批影像关联、包外引用和并发上传。

## 一、快速启动

```bash
docker compose up -d --build
# 交互式 API 文档（Swagger UI）
open http://localhost:8080/docs          # 经 nginx 轮询两个 API 进程
# 或绕过负载均衡直接访问单个副本
open http://localhost:8001/docs          # api1
open http://localhost:8002/docs          # api2
```

一键端到端演示（跨包引用 + 并发上传 + 失败回滚 + 重启复用，全部自动断言）：

```bash
docker compose run --rm demo
```

## 二、目录结构

```
app/
  config.py        运行配置（数据目录、20 份/200MB 限制、UID 前缀等）
  database.py      SQLite 表结构、预留/提交/回滚事务、启动清理
  identifiers.py   随机 UID/患者ID 生成、标识采集、DICOM 匿名化改写
  pipeline.py      ZIP 安全解压 → 校验 → 预留映射 → 改写打包 → 提交
  main.py          FastAPI 路由（/docs、上传、状态、下载、映射查询、样例）
scripts/
  generate_samples.py  合成样例生成器（无任何真实个人信息）
  demo.py              纯标准库端到端演示脚本
nginx/nginx.conf   两副本 round-robin 负载均衡
tests/             pytest 测试（并发收敛、回滚、完整管道、路径穿越等）
docker/entrypoint.sh
Dockerfile
docker-compose.yml
```

## 三、处理规则（与需求逐条对应）

| 需求 | 实现 |
| --- | --- |
| 非空 PatientID 随机持久映射 | `id_mappings` 表，类型 `patient`，值形如 `ANON-<16位hex>` |
| Study/Series/SOPInstanceUID 随机持久映射 | OID `2.25.<uuid4十进制>`，≤64 字符，全局唯一，落库后永久复用 |
| ReferencedSOPInstanceUID 同一映射递归替换 | 递归遍历所有 Sequence，与 SOPInstanceUID 共用 `instance` 映射表 |
| 包外引用也先分配 | 采集阶段即收集全部引用 UID（含包内不存在的），预留时一并建映射 |
| 同步文件元信息实例 UID | `file_meta.MediaStorageSOPInstanceUID` 同步为新 SOP UID |
| 保留类 UID / 传输语法 / 像素字节 | 不改 SOPClassUID、TransferSyntaxUID，不转码，`PixelData` 原样写回 |
| 清空 PatientName、PatientBirthDate | 标签保留、值置空（零长度） |
| 删除私有标签 | `Dataset.remove_private_tags()`，含私有创建者标签 |
| 仅接受未压缩 DICOM | 仅放行 Little Endian Implicit/Explicit（及 Big Endian），压缩/Deflate 拒绝 |
| 每包最多 20 份 DICOM | 超限整包失败，不产生输出、不留新映射 |

## 四、并发与一致性设计（重点）

两个 API 进程共享一个 SQLite 文件（WAL 模式）。映射的生命周期分两阶段：

1. **预留（短写事务）** — `BEGIN IMMEDIATE` 事务内，对包内全部标识（含包外引用）
   逐条“查已有行，没有则插入并把 `owner_job` 记为当前任务、`committed=0`”，事务
   **立即提交结束**。SQLite 写事务天然串行化：两个包并发首次遇到同一标识时，
   先到者建行，后到者直接复用，因此两包必然得到同一映射。后来的任务复用他人预留
   行（re-adopt）后，自身失败也不会删除该行。
2. **改写打包（不持任何数据库写锁）** — DICOM 读取、标识替换、ZIP 打包全程没有
   打开的写事务，其他进程的上传不会被阻塞。
3. **提交（短写事务）** — 只有完整 ZIP 生成成功后，才把本任务拥有的行批量置为
   `committed=1, owner_job=NULL`，同时任务置 `done`，下载接口随即开放。
4. **失败回滚** — 任何阶段失败都会删除仍归本任务所有的未提交行并把任务置
   `failed`；已提交的历史映射和被他人复用的行不受影响。
5. **重启恢复** — 启动时 `sweep_stale_jobs` 清理上次中断任务残留的未提交预留；
   已提交映射永久保留，重启后继续沿用（演示脚本最后一步会验证 UID 完全不变）。

下载接口在 `status != done` 时返回 `409`，失败任务拿不到任何产物。

## 五、API 一览（/docs 可交互试用）

| 方法/路径 | 说明 |
| --- | --- |
| `POST /jobs` | 上传 ZIP（multipart 字段 `file`），返回 `job_id`，后台处理 |
| `GET /jobs/{id}` | 查询任务状态 `processing / done / failed`（含失败原因） |
| `GET /jobs/{id}/download` | 成功后下载匿名化 ZIP；未完成/失败返回 409 |
| `GET /jobs/{id}/report` | 每文件 原始值→匿名值 的映射报告 JSON |
| `GET /jobs` | 最近任务列表 |
| `GET /mappings?id_type=...` | 查询**已提交**的持久映射（仅已提交，不含在途预留） |
| `GET /mappings/stats` | 各类型已提交数量 + 在途未提交预留数量 |
| `GET /samples` / `GET /samples/{name}` | 下载内置合成样例 ZIP |
| `GET /health` | 副本标识与库统计（经 nginx 反复调用可观察轮询到 api1/api2） |

## 六、合成样例说明（无真实个人信息）

镜像构建时由 `scripts/generate_samples.py` 生成，全部标识、姓名、像素帧均为脚本
虚构：

- `sample_crossref_a.zip` — 患者 `HOSP01-P-000999`，研究 A，2 张 64×64 CT
- `sample_crossref_b.zip` — **同一患者**的另一研究（跨批关联），其
  `ReferencedSOPSequence` 同时引用：① A 包内的 SOP（跨包引用）；② 任何包都不
  包含的 SOP（包外引用）
- `sample_too_many.zip` — 21 份 DICOM，故意超过 20 份上限，用于演示整包失败与
  映射回滚

在 `/docs` 里可依次：`GET /samples/{name}` 下载 → `POST /jobs` 上传（两个样例
同时上传即可演示并发与跨包引用）→ 轮询 `GET /jobs/{id}` →
`GET /jobs/{id}/download` → `GET /mappings` 核对两包共用同一匿名患者 ID。

## 七、本地开发与测试

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python scripts/generate_samples.py --out ./data/samples
DICOM_DATA_DIR=./data REPLICA_ID=api1 uvicorn app.main:app --port 8001
DICOM_DATA_DIR=./data REPLICA_ID=api2 uvicorn app.main:app --port 8002
python -m pytest -q
python scripts/demo.py --base-url http://127.0.0.1:8001
```

## 八、安全注意事项

- 上传 ZIP 在每个任务独立的空目录中解压，禁止绝对路径、盘符与 `..` 路径穿越；
  对解压总大小（默认 500MB）和上传大小（默认 200MB）设限。
- 严格 DICOM 读取（要求 DICM 前导标记），非 DICOM 文件整包拒绝。
- 映射查询接口只暴露已提交数据，处理中的预留不对外可见。
- 样例数据为完全合成数据，不含任何真实个人信息。
