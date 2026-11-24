# 边缘录像卷回收台（edge-rollover）

Flask 实现的边缘录像卷回收页。录像写入**三份等容量的本地分区镜像**，其中始终
保留**一份作为周转区**；网页支持追加短录像、下载、标记留存、封区回收，并保证
在途下载读完原字节后才清空旧区。封区卷还可以**离线拆盘检修**，装回时核对卷
持久标识与每条登记录像的大小、摘要，确认是原卷后才恢复原下载链接与回收。

## 1. 模型与状态机

三个分区 `p1 / p2 / p3`，容量均为 `PARTITION_CAPACITY`（默认 64 MiB）。
每个分区在容器内是一个挂载点（loop 镜像 + ext4），无特权时自动降级为普通目录，
容量由应用按元数据记账强制限制，行为一致。

```
 idle ──提升为写入区──▶ active ──封区──▶ sealed ──发起回收──▶ recycling
  ▲                                       │  ▲                    │
  │                                       │  └── 回收先获准：离线驳回
  │                                       ├──无在途下载·离线─▶ offline
  │                                       │                    │
  │                                       │   原卷装回+标识/大小/摘要
  │                                       │   核验+复检仍是原卷 ◀┘
  │                                       └──核验通过回到 sealed
  │                                    搬移全部成功     │ 搬移失败（原数据不动，
  └──────────── 清空（在途下载归零）──── draining ◀─────┘ 清理周转区后可重试）
```

- `active`：唯一写入区，接受新录像追加；
- `sealed`：封区，停止写入，等待回收；封区同时把一个 `idle` 周转分区提升为新 `active`；
  只有 `sealed` 且**无在途下载**时可以离线拆卷；
- `offline`：卷已离线检修，拒绝该卷的新下载与回收；状态写 SQLite，**重启保持**；
  装回原卷并通过持久标识 + 全量大小/sha256 核验、且恢复前标识复检一致后才回到 `sealed`；
  卷没装回、装错卷、文件缺失/大小或摘要不符、核验期间被换卷，都继续保持 `offline`；
- `recycling`：留存片段正在搬入当前写入区（搬入区）；
- `draining`：搬移完成，未留存片段停止接受新下载，等待在途下载结束；
- `idle`：已清空的周转分区，留给下次周转。

## 2. 关键语义（与需求逐条对应）

| 需求 | 实现 |
| --- | --- |
| 追加短录像 | `POST /api/recordings`，流式落盘到 active 分区，sha256 + 回读校验，容量不足拒绝且无半截文件 |
| 下载 / 标记留存 | 下载支持 Range；标记只改元数据，任意状态可操作 |
| 封区后发起回收 | `seal` 先提升周转分区为新写入区，再对旧区 `recycle` |
| 标记片段全搬到周转区后，原链接从新位置取录像 | 逐文件 `复制→哈希校验→原子 rename→切换 current_partition`，录像 ID 不变，下载链接不变，服务端自动指向新位置 |
| 其余片段停止接受新下载 | recycling/draining 分区上未留存片段的新下载请求返回 409 |
| 新录像可追加到搬入区 | 搬入区就是新 active，上传与搬移可并发（staging 按源分区隔离） |
| 已开始的下载须读完原字节 | 下载开始即登记 token 与物理分区，流式读原路径；客户端断开登记 interrupted |
| 旧区等下载全结束才可清空 | draining 分区活动下载数 >0 时拒绝清空；后台线程轮询 + 手动接口双保险，归零后 wipe 回 idle |
| 搬移写入失败，原目录和录像照常可用 | 失败只产生/清理 `.part` 临时文件，源文件只读不删、元数据不切；分区留在 recycling |
| 清理周转区后可重试 | cleanup 清残留并回滚已提交片段的归属（删除孤儿副本），回收接口幂等，已搬好的文件自动跳过 |
| 封区卷可离线拆盘，且必须无在途下载 | `POST /api/partitions/<pid>/offline`：在 `db.lock` 临界区内校验 `state=sealed` 且活动下载数为 0 才转 `offline` |
| 请求交错：下载/回收先获准，离线驳回 | 下载“准入判定 + token 登记”合并为同一把锁内的原子操作 `service.admit_download`；回收在同一把锁内完成 `sealed→recycling`。谁先提交谁生效：先有活动 token 则离线看到计数>0 驳回；先转 offline 则下载/回收按 offline 拒绝；先转 recycling 则离线按“回收已获准”驳回 |
| 离线后拒绝新下载/回收，重启保持 | 下载准入与回收发起都显式拒绝 `offline`；状态写入 SQLite `partitions.state='offline'`，启动引导跳过 offline 分区，不在其挂载点创建任何文件/目录 |
| 卷上持久标识 | 每卷根目录 `.rollover-volume.json` 写 `volume_id`（首次启动生成，DB 同存）；loop 模式在 ext4 上随镜像走，目录模式随整个目录 mv/cp |
| 装回核对标识 + 每条录像大小/摘要 | `POST /api/partitions/<pid>/reattach`：先读持久标识与 DB 原卷 ID 比对，再对 `current_partition=<pid>` 的每条登记录像核对存在性、字节数、sha256；哈希 IO 在全局元数据锁外执行，核验期间其他分区照常读写 |
| 确认仍是刚核验的原卷才恢复 | 全部核验通过后，在 `db.lock` 临界区内重读卷标识并复检状态，一致才 `offline→sealed`；原录像 ID/链接/relpath 未变，下载与回收自然恢复 |
| 没装回/装错/核验失败保持离线 | 分别返回 `volume_absent` / `no_identity`(`bad_identity`) / `wrong_volume` / `verification_failed` / `volume_changed_during_verify`，HTTP 409 并带结构化 `report`，同时写 `last_error` 与事件日志 |

## 3. 目录结构

```
edge-rollover/
├── app/
│   ├── config.py        # 环境变量配置
│   ├── db.py            # SQLite 元数据（分区/录像/下载会话/事件日志）
│   ├── storage.py       # 文件落盘、哈希校验、搬移、staging 隔离、故障注入
│   ├── service.py       # 分区状态机与全部业务规则
│   ├── web.py           # Flask 路由与流式下载
│   ├── templates/index.html
│   └── static/style.css
├── tests/e2e_test.py             # 端到端验收（75+ 项，含离线/交错/错盘损坏场景）
├── tests/offline_restart_e2e.py  # 离线重启保持 + 重启后错盘 + 原卷接回（自管进程）
├── docker/entrypoint.d/10-partitions.sh  # loop 镜像初始化/目录降级
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── run.py
```

## 4. 启动

### Docker Compose（推荐，目录模式即可模拟拔盘/错装）

```bash
docker compose up -d --build
# 打开 http://localhost:8080
```

- **compose 默认 `USE_LOOP_IMAGES=0`（目录模式）且不带特权**，任意 Docker 环境
  都能起；三个“卷”就是命名卷 `rollover-data` 里的 `p1/p2/p3` 目录，拔盘/错装
  直接 `docker exec ... mv/cp` 改名/复制目录即可（见下节）。
- 需要真机 loop 镜像 + ext4 时：
  `USE_LOOP_IMAGES=1 PRIVILEGED=true docker compose up -d --build`
  （compose 已用 `${USE_LOOP_IMAGES:-0}` / `${PRIVILEGED:-false}` 参数化）。
- 数据保存在命名卷 `rollover-data`，`restart: unless-stopped`。

### 离线 / 拔盘 / 错装 / 原卷接回演练（目录模式）

先在页面封 `p1`（或 `POST /api/partitions/seal`），再让 `p1` 无在途下载并点
“离线检修”（或 `POST /api/partitions/p1/offline`）。之后：

```bash
C=edge-rollover
# 1) 拔盘：目录移走；再模拟 entrypoint 重建空挂载点
docker exec $C sh -c 'mv /data/p1 /data/p1.orig && mkdir -p /data/p1'
curl -s -X POST localhost:8080/api/partitions/p1/reattach   # 409 volume_absent/no_identity

# 2) 重启：offline 必须保持（entrypoint 的 mkdir -p 不会初始化离线卷）
docker compose restart

# 3) 重启后装错盘：把带另一持久标识的 p3 复制成 p1
docker exec $C sh -c 'rm -rf /data/p1 && cp -a /data/p3 /data/p1'
curl -s -X POST localhost:8080/api/partitions/p1/reattach   # 409 wrong_volume，仍 offline

# 4) 原卷装回并核验：标识 + 每条录像大小/sha256 全过 -> sealed，链接/回收恢复
docker exec $C sh -c 'rm -rf /data/p1 && mv /data/p1.orig /data/p1'
curl -s -X POST localhost:8080/api/partitions/p1/reattach   # 200, checked=<登记数>
```

损坏核验可用（应返回 `verification_failed` 且保持 offline）：

```bash
docker exec $C sh -c 'echo x >> /data/p1/recordings/<rid>.bin'      # 大小不符
docker exec $C sh -c "printf X | dd of=/data/p1/recordings/<rid>.bin \
  bs=1 seek=10 count=1 conv=notrunc"                                # 同长度篡改->摘要不符
```

> 直接本地跑（见下）时把 `docker exec $C sh -c '...'` 换成对 `$DATA_DIR/p1`
> 的同样操作即可；`tests/offline_restart_e2e.py` 已把上述“拔盘→重启→错盘→
> 再重启→原卷接回”全过程自动化。

### 本地直接运行（无 Docker 时）

```bash
pip install -r requirements.txt
DATA_DIR=./data DB_PATH=./data/meta.db \
PARTITION_CAPACITY=$((64*1024*1024)) ENABLE_TEST_HOOKS=1 \
python run.py
```

## 5. HTTP API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 管理页面 |
| GET | `/api/overview` | 分区/录像/下载/事件总览 |
| POST | `/api/recordings` | multipart 上传（字段 `file`，可选 `retained/size/sha256`） |
| POST | `/api/recordings/<rid>/retained` | 标记/取消留存 `{"retained": true}` |
| GET | `/api/recordings/<rid>/download` | 下载（支持 Range，响应头带 `X-Download-Token`、`X-Source-Partition`） |
| POST | `/api/partitions/seal` | 封区并提升周转分区 |
| POST | `/api/partitions/<pid>/offline` | 封区卷离线检修（须 sealed 且无在途下载；别名 `/eject`） |
| POST | `/api/partitions/<pid>/reattach` | 装回原卷并核验接回（别名 `/online`、`/attach`），失败返回 409 + `report` |
| POST | `/api/partitions/<pid>/recycle` | 发起/重试回收（offline 卷返回 409） |
| POST | `/api/partitions/<pid>/cleanup` | 失败后清理周转区并回滚 |
| POST | `/api/partitions/<pid>/drain` | 立即检查排空（后台也会自动轮询） |
| GET | `/api/downloads` | 下载会话流水 |
| POST | `/api/test/fail-move` / `/api/test/reset-fail` | 故障注入（仅 `ENABLE_TEST_HOOKS=1`） |
| POST | `/api/test/verify-delay` `{"seconds":0.4}` | 放大接回核验窗口，用于观察核验期间并发读写（仅测试钩子） |

下载链接加 `?slow=<秒>` 可按分块节流，用于复现“下载中回收”。

## 6. 验收测试

```bash
# 先启动服务（目录模式，测试与服务共享 DATA_DIR），然后：
BASE_URL=http://127.0.0.1:8080 python -I tests/e2e_test.py

# 离线/重启专项：脚本自管临时 DATA_DIR 与 run.py 子进程，
# 自动完成“离线→拔盘→重启→空挂载点→错盘→错盘再重启→原卷接回→回收恢复”：
python -I tests/offline_restart_e2e.py
```

`e2e_test.py` 覆盖：上传/容量/Range/标记、封区轮转、**下载中回收**（慢速下载
未结束分区不清空、结束后自动排空）、链接切换、未留存片段拒绝新下载、**搬移失败**
（原文件可用、清理回滚、幂等重试）、两轮周转闭环；以及**离线检修专项**：

- 有在途下载时离线驳回、下载收尾后离线成功；
- 离线后新下载/回收/重复离线均 409；
- 拔盘(`volume_absent`)、空挂载点(`no_identity`)、错盘(`wrong_volume`)、
  文件缺失/大小不符/sha256 篡改均 `verification_failed` 且保持 offline；
- 原卷接回后核验条数覆盖全部登记、原链接字节一致、回收恢复并排空；
- 下载 vs 离线真并发竞态的唯一赢家不变量（连跑多轮，两种先后分支都出现过）；
- 回收先获准（注入搬移失败停在 recycling）时离线被驳回；
- `verify-delay` 放大核验窗口期间，其他分区上传/下载成功、overview 毫秒级响应。

### 本次交付的实测结果（2026-11-24，本机 Linux，Python 3.11.2 + Flask 3.1.3）

| 试跑项 | 命令/方式 | 结果 |
| --- | --- | --- |
| 全量 e2e（含原 36 项与新增离线/竞态断言，共 75/76 项） | 全新临时 `DATA_DIR` 起 `run.py`，`BASE_URL=… python tests/e2e_test.py` | 连续 4 次全新库运行全部 `通过 75 / 失败 0`（下载优先分支多 1 条断言为 76），两种竞态分支均观测到 |
| 重启后错盘 + 原卷接回专项 | `python tests/offline_restart_e2e.py` | 连续 3 次 `通过 22 / 失败 0` |
| 旧版本库迁移 | 手工构造旧 `partitions` 表启动新代码 | 3 个新列自动补齐、卷标识生成，封区/离线/接回(`checked=1`)通过 |
| 前端内联 JS | 抽取 `<script>` 后 `node --check` | 语法 OK |
| compose / entrypoint | PyYAML 解析 + `sh -n` | YAML、shell 语法 OK |

> 说明：e2e 的拔盘/错盘/损坏用 `os.rename/copytree` 直接操作 `mount_path`，
> 需与服务共享 `DATA_DIR`（本地目录模式，或在容器内/`docker exec` 运行）；
> 从宿主机打 Docker 端口且看不到 `/data` 时，这些目录模拟断言会标记 `SKIP`，
> HTTP 离线/接回/恢复断言仍会执行。重启专项脚本始终在本机文件系统内自管进程，
> 不依赖 Docker。

## 7. 已知限制 / 未完成项

- **本沙箱环境没有 docker / docker daemon**，因此未实际执行
  `docker compose up --build`；compose 已通过 YAML 解析、entrypoint 通过
  `sh -n`，拔盘/错装/重启/接回全部在等价的本地目录模式下实测通过。
  loop 镜像 + 特权挂载路径仍需在有 Docker/特权的真机上首跑确认
  （本次把 compose 默认改成了免特权的目录模式）。
- 接回前的“仍是原卷”以**卷根持久标识文件的读前 + 核验后临界区内复检**为准，
  不识别内核块设备/inode 级身份；目录模式下这是合适的模拟粒度，loop/真机模式
  若需更强保证可再加 `blkid`/挂载源 `stat` 比对。
- 使用 Flask 内置 WSGI 服务器（多线程），适合边缘单机小规模场景；高并发部署
  应换 waitress/gunicorn，多进程需改用文件锁/外部锁。
- 容量记账基于元数据 + 目录扫描，未做磁盘 inode 配额；loop 镜像本身有 ext4
  硬上限兜底，目录降级模式只有应用层限制。
- 下载不做鉴权（内网边缘工具定位）；如需公网使用要在前置代理加认证。
- SQLite 单文件元数据，未做主动备份；WAL 模式下可直接备份卷目录。
