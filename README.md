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

入库高峰专项演示（同样全自动断言）：

```bash
# 持锁：两副本各 40 笔上传等锁时，读接口仍 <1s；忙等超时返回可重试 503 且无残留
docker compose run --rm demo-locking
# 取消：排队中断不迟到登记；已受理任务在响应断连后继续跑完
docker compose run --rm demo-cancel
```

## 二、目录结构

```
app/
  config.py        运行配置（数据目录、20 份/200MB 限制、30s 忙等、有界受理参数）
  database.py      SQLite 表结构、预留/提交/回滚事务、忙等分类、带重试的失败登记、启动清理
  identifiers.py   随机 UID/患者ID 生成、标识采集、DICOM 匿名化改写
  pipeline.py      ZIP 安全解压 → 校验 → 预留映射 → 改写打包 → 提交（失败清理独占暂存）
  runner.py        专属有界工作线程池 + 有界队列 + 取消语义（不碰共享线程池）
  main.py          FastAPI 路由（/docs、上传、状态、下载、映射查询、样例、503 处理）
scripts/
  generate_samples.py  合成样例生成器（无任何真实个人信息）
  demo.py              纯标准库端到端演示脚本
  demo_locking.py      持锁高峰演示（40+40 等锁、读 <1s、503、无残留）
  demo_cancel.py       取消上传演示（排队取消不迟到登记、受理后断连继续处理）
  run_cancel_demo.sh   取消演示的临时副本启停封装（compose 调用）
nginx/nginx.conf   两副本 round-robin 负载均衡
tests/             pytest 测试（并发收敛、回滚、完整管道、持锁/503/取消、路径穿越等）
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
   `failed`；已提交的历史映射和被他人复用的行不受影响。回滚本身在锁竞争下带
   有界重试（`FAIL_JOB_RETRIES`），仍失败则交由启动清扫兜底。
5. **重启恢复** — 启动时 `sweep_stale_jobs` 清理上次中断任务（含已登记但无人
   执行的任务）残留的未提交预留、`processing` 行与独占暂存目录；已提交映射
   永久保留，重启后继续沿用。

下载接口在 `status != done` 时返回 `409`，失败任务拿不到任何产物。

### 4.1 入库高峰：有界受理与 30 秒忙等待

上传登记（`jobs` 表插入）与整个处理管道运行在 **每个副本专属的有界工作线程池**
（`app/runner.py`，默认 `UPLOAD_WORKERS=8`）上，**不使用也不扩大** anyio/Starlette
共享线程池，更不占用事件循环线程：

- 上传先进入有界内存队列（默认 `UPLOAD_QUEUE_LIMIT=64`），等待占用的执行资源
  因此始终有界；队列已满时新上传立即得到可重试的 `503`（带 `Retry-After: 5`）。
- 登记写事务遇到写锁时，在专属工作线程内按 SQLite `busy_timeout` **忙等待 30 秒**
  （`SQLITE_BUSY_TIMEOUT_MS=30000`，沿用既有约定）。HTTP 协程只在 `asyncio.Future`
  上等待，本身不占工作线程。
- WAL 的读不阻塞写、写不阻塞读：两副本各有 40 笔上传等锁时，`/health`、
  `GET /jobs`、`GET /mappings`、`GET /mappings/stats` 仍在 1 秒内返回已提交数据
  （实测毫秒级）。
- 每个数据库连接都由上下文管理器保证关闭；工作线程只有在当前数据库操作完成
  提交或回滚、连接关闭之后才归还名额。

### 4.2 锁超时与取消语义

- **未受理上传锁超时**：30 秒仍拿不到写锁 → 返回可重试 `503`，不产生任何
  `processing` 行，并删除该上传独占的暂存目录与 spool 文件。
- **已受理后台任务锁超时**：任务已登记后，预留/提交等阶段再遇锁超时 → 任务
  记为 `failed`，删除其独占的新映射（未提交预留）与整个暂存目录；被他人复用
  或已提交的映射不受影响。
- **请求取消（排队阶段）**：客户端在登记前断连，工作线程在触碰数据库前检查
  取消标志，直接丢弃该上传，**排队工作不会迟到登记**。
- **请求取消（受理之后）**：一旦登记成功，上传响应即使断连，已受理的影像处理
  仍在后台运行到 `done`/`failed`。
- 演示用登记门闩（`UPLOAD_REGISTRATION_GATE`，默认不设置即完全无副作用）仅用于
  在 `demo-cancel` 中确定性复现“排队中断”。

## 五、API 一览（/docs 可交互试用）

| 方法/路径 | 说明 |
| --- | --- |
| `POST /jobs` | 上传 ZIP（multipart 字段 `file`），返回 `job_id`，后台处理；队列满或锁忙等超时返回 **503 + `Retry-After`**（可重试），响应含 `queue_depth` |
| `GET /jobs/{id}` | 查询任务状态 `processing / done / failed`（含失败原因） |
| `GET /jobs/{id}/download` | 成功后下载匿名化 ZIP；未完成/失败返回 409 |
| `GET /jobs/{id}/report` | 每文件 原始值→匿名值 的映射报告 JSON |
| `GET /jobs` | 最近任务列表 |
| `GET /mappings?id_type=...` | 查询**已提交**的持久映射（仅已提交，不含在途预留） |
| `GET /mappings/stats` | 各类型已提交数量 + 在途未提交预留数量 |
| `GET /samples` / `GET /samples/{name}` | 下载内置合成样例 ZIP |
| `GET /health` | 副本标识与库统计（经 nginx 反复调用可观察轮询到 api1/api2）；等锁期间仍 <1s 返回 |

上传相关状态码：`202` 已受理（后台继续，响应断连不影响处理）、`413` 超限、
`415` 非 ZIP、`503` 队列饱和或 30 秒忙等超时（可重试）。

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

# 持锁高峰演示（默认连本机 8001/8002；演示用 3s 忙等以便快速观察 503）
SQLITE_BUSY_TIMEOUT_MS=3000 # 仅演示时调小；生产保持默认 30000
python scripts/demo_locking.py
# 取消演示（脚本会自行启动一个带登记门闩的临时副本并在结束时回收）
DICOM_DATA_DIR=./data sh scripts/run_cancel_demo.sh
```

受理与忙等待相关环境变量（均有默认值，生产通常无需修改）：

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `SQLITE_BUSY_TIMEOUT_MS` | `30000` | SQLite 写锁忙等待时长（30 秒），超时未受理上传返回 503 |
| `UPLOAD_WORKERS` | `8` | 每副本专属上传/处理工作线程数（不动共享线程池） |
| `UPLOAD_QUEUE_LIMIT` | `64` | 有界等待队列容量，超出立即 503 |
| `REGISTRATION_TIMEOUT_S` | `35` | 请求侧登记看门狗（高于忙等超时，正常不触发） |
| `FAIL_JOB_RETRIES` | `3` | 失败回滚登记在锁竞争下的有界重试次数 |
| `UPLOAD_REGISTRATION_GATE` | 未设置 | 仅取消演示使用的登记前门闩文件，不设置则无任何影响 |

## 八、安全注意事项

- 上传 ZIP 在每个任务独立的空目录中解压，禁止绝对路径、盘符与 `..` 路径穿越；
  对解压总大小（默认 500MB）和上传大小（默认 200MB）设限。
- 严格 DICOM 读取（要求 DICM 前导标记），非 DICOM 文件整包拒绝。
- 映射查询接口只暴露已提交数据，处理中的预留不对外可见。
- 样例数据为完全合成数据，不含任何真实个人信息。
