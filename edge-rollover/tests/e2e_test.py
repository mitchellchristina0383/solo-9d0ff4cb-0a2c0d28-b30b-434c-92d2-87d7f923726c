# -*- coding: utf-8 -*-
"""端到端验收脚本（不依赖 pytest，直接 python 运行）。

用法：
    BASE_URL=http://127.0.0.1:8080 python -I tests/e2e_test.py

覆盖：
  1. 上传/容量/下载/Range/标记留存基础流程
  2. 封区 + 周转分区提升为新写入区，新录像可继续追加
  3. 回收：留存片段搬入写入区，原链接从新位置取录像；
     未留存片段停止接受新下载；
  4. 下载中回收：慢速下载未结束时源分区保持 draining 不被清空，
     下载读完原字节结束后分区自动清空回 idle；
  5. 搬移失败注入：原目录与原录像照常可用，清理周转区后重试成功。
  6. 卷离线/接回：在途下载拦截、下载/回收交错竞态、离线后拒绝新下载与回收、
     拔盘/空挂载点/错盘/大小或摘要损坏均保持离线、原卷接回后恢复链接与回收、
     核验期间其他分区照常读写。

注意：场景 6 的拔盘/错装用目录改名/复制模拟，要求测试进程与服务共享同一
DATA_DIR 文件系统（本地目录模式，或在容器内 / docker exec 运行本脚本）。
"""
import hashlib
import json
import os
import shutil
import sys
import time
import urllib.request
import urllib.error
import uuid

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8080").rstrip("/")
PASS, FAIL, SKIP = 0, 0, 0
RESULTS = []


def check(name: str, cond: bool, detail: str = "") -> bool:
    global PASS, FAIL
    tag = "PASS" if cond else "FAIL"
    if cond:
        PASS += 1
    else:
        FAIL += 1
    line = f"[{tag}] {name}" + (f" -- {detail}" if detail else "")
    print(line, flush=True)
    RESULTS.append((tag, name, detail))
    return bool(cond)


def skip(name: str, detail: str = "") -> None:
    global SKIP
    SKIP += 1
    print(f"[SKIP] {name}" + (f" -- {detail}" if detail else ""), flush=True)
    RESULTS.append(("SKIP", name, detail))


def req(method: str, path: str, data=None, headers=None, raw=False,
        timeout=30):
    url = BASE + path
    body = None
    h = headers or {}
    if data is not None and not isinstance(data, (bytes, bytearray)):
        body = json.dumps(data).encode()
        h.setdefault("Content-Type", "application/json")
    elif data is not None:
        body = data
    r = urllib.request.Request(url, data=body, headers=h, method=method)
    try:
        resp = urllib.request.urlopen(r, timeout=timeout)
        payload = resp.read()
        if raw:
            return resp.status, dict(resp.headers), payload
        return resp.status, dict(resp.headers), json.loads(payload or b"{}")
    except urllib.error.HTTPError as e:
        payload = e.read()
        if raw:
            return e.code, dict(e.headers), payload
        try:
            return e.code, dict(e.headers), json.loads(payload)
        except json.JSONDecodeError:
            return e.code, dict(e.headers), {"error": payload.decode(errors="replace")}


def multipart(filename: str, content: bytes, retained: bool = False,
              sha256: str | None = None):
    boundary = "----e2e" + uuid.uuid4().hex
    lines = []
    def field(name, value):
        lines.append(f"--{boundary}\r\nContent-Disposition: form-data; "
                     f"name=\"{name}\"\r\n\r\n{value}\r\n".encode())
    field("size", str(len(content)))
    field("retained", "1" if retained else "0")
    if sha256:
        field("sha256", sha256)
    lines.append(
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; "
        f"filename=\"{filename}\"\r\nContent-Type: application/octet-stream\r\n\r\n".encode()
    )
    lines.append(content)
    lines.append(f"\r\n--{boundary}--\r\n".encode())
    return b"".join(lines), {"Content-Type": f"multipart/form-data; boundary={boundary}"}


def upload(name: str, content: bytes, retained: bool = False) -> dict:
    body, headers = multipart(name, content, retained)
    status, _, data = req("POST", "/api/recordings", body, headers)
    assert status == 201, f"upload failed {status}: {data}"
    return data


def overview() -> dict:
    _, _, ov = req("GET", "/api/overview")
    return ov


def wait_state(pid: str, state: str, timeout: float = 20) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ov = overview()
        for p in ov["partitions"]:
            if p["id"] == pid:
                if p["state"] == state:
                    return p
        time.sleep(0.5)
    raise AssertionError(f"partition {pid} did not reach state {state}: "
                         f"{overview()['partitions']}")


def part(ov, pid):
    return next(p for p in ov["partitions"] if p["id"] == pid)


def reset_world():
    """回到干净的三分区初始态：需要全新数据目录启动服务。"""
    ov = overview()
    states = sorted(p["state"] for p in ov["partitions"])
    check("初始分区数为 3", len(ov["partitions"]) == 3)
    check("初始恰好一个 active 分区",
          sum(1 for p in ov["partitions"] if p["state"] == "active") == 1,
          str(states))


def test_basic_flow():
    print("\n=== 场景1: 上传/下载/标记/Range/容量 ===")
    payload = bytes((i * 7 + 3) % 256 for i in range(200_000))
    digest = hashlib.sha256(payload).hexdigest()
    rec = upload("clip-a.bin", payload)
    check("上传后 sha256 正确", rec["sha256"] == digest)
    check("上传后落在 active 分区", rec["current_partition"] == "p1",
          rec["current_partition"])

    status, headers, body = req("GET", f"/api/recordings/{rec['id']}/download",
                                raw=True)
    check("下载状态 200 且字节一致", status == 200 and body == payload,
          f"status={status} len={len(body)}")
    check("下载响应带分区溯源头", headers.get("X-Source-Partition") == "p1")

    status, headers, body = req(
        "GET", f"/api/recordings/{rec['id']}/download",
        headers={"Range": "bytes=100-199"}, raw=True)
    check("Range 请求返回 206 且内容正确",
          status == 206 and body == payload[100:200]
          and headers.get("Content-Range", "").startswith("bytes 100-199/"),
          f"status={status} cr={headers.get('Content-Range')}")

    status, _, data = req("POST", f"/api/recordings/{rec['id']}/retained",
                          {"retained": True})
    check("标记留存成功", status == 200 and data["retained"] == 1)

    # 容量限制：上传一个超过 64MiB 的文件（服务端默认容量）
    ov = overview()
    cap = part(ov, "p1")["capacity"]
    big = b"\x00" * (cap + 1024)
    body, headers = multipart("too-big.bin", big)
    status, _, data = req("POST", "/api/recordings", body, headers)
    check("超容量上传被拒绝且无残留", status in (409, 413),
          f"status={status} resp={data}")
    ov = overview()
    check("拒绝后分区已用量未增加",
          part(ov, "p1")["used"] == 200_000, f"used={part(ov,'p1')['used']}")
    return rec


def test_recycle_during_download():
    print("\n=== 场景2: 封区回收 + 下载中回收（核心） ===")
    # p1 上准备：1 个留存片段 + 1 个未留存片段（较大，供慢速下载）
    keep = upload("keep.bin", b"KEEP-" * 50_000, retained=True)   # 300KB
    drop = upload("drop.bin", os.urandom(8 * 1024 * 1024), retained=False)  # 8MB
    ov = overview()
    check("回收前两个录像均在 p1",
          keep["current_partition"] == "p1" and drop["current_partition"] == "p1")

    # 先发起一个慢速下载（未留存片段），制造“已开始的下载”
    import threading
    dl_result = {}
    def slow_download():
        try:
            status, headers, body = req(
                "GET",
                f"/api/recordings/{drop['id']}/download?slow=0.3",
                raw=True, timeout=120)
            dl_result["status"] = status
            dl_result["ok"] = (len(body) == 8 * 1024 * 1024)
            dl_result["token"] = headers.get("X-Download-Token")
            dl_result["partition"] = headers.get("X-Source-Partition")
        except Exception as exc:
            dl_result["error"] = repr(exc)
    t = threading.Thread(target=slow_download)
    t.start()
    # 等连接建立并登记为活动下载（轮询，避免时序抖动）
    deadline = time.time() + 10
    while time.time() < deadline:
        if any(d["recording_id"] == drop["id"]
               for d in overview()["active_downloads"]):
            break
        time.sleep(0.2)

    # 封区：p1 -> sealed，p2 -> active
    status, _, data = req("POST", "/api/partitions/seal")
    check("封区成功并提升周转分区", status == 200
          and data["sealed"] == "p1" and data["new_active"] == "p2", str(data))
    wait_state("p2", "active")

    # 新录像可追加到搬入区 p2
    new_rec = upload("new-on-p2.bin", b"NEW-AREA")
    check("封区后新录像写入新 active(p2)", new_rec["current_partition"] == "p2")

    # 发起回收（场景1 中标记留存的 clip-a 也在 p1，会一并搬入 p2）
    status, _, data = req("POST", "/api/partitions/p1/recycle")
    check("回收接口返回 draining", status == 200 and data["status"] == "draining",
          str(data))
    ov = overview()
    p1_retained_ids = {r["id"] for r in ov["recordings"]
                       if r["source_partition"] == "p1" and r["retained"] == 1}
    check("p1 全部留存片段已搬到 p2",
          set(data.get("moved", [])) == p1_retained_ids
          and keep["id"] in p1_retained_ids,
          f"moved={data.get('moved')} expected={p1_retained_ids}")

    ov = overview()
    p1 = part(ov, "p1")
    check("p1 进入 draining 且慢速下载仍在途",
          p1["state"] == "draining" and p1["active_downloads"] >= 1,
          f"state={p1['state']} dl={p1['active_downloads']}")

    # 原链接（同一录像 ID）现在从新位置 p2 取录像
    status, headers, body = req(
        "GET", f"/api/recordings/{keep['id']}/download", raw=True)
    check("留存片段原链接已切换到新位置取流",
          status == 200 and headers.get("X-Source-Partition") == "p2"
          and body == b"KEEP-" * 50_000,
          f"status={status} src={headers.get('X-Source-Partition')}")
    ov = overview()
    keep_now = next(r for r in ov["recordings"] if r["id"] == keep["id"])
    check("元数据 current_partition 已切换为 p2",
          keep_now["current_partition"] == "p2" and keep_now["state"] == "moved")

    # 未留存片段停止接受新下载（在途的那个慢速下载不受影响）
    status, _, data = req(
        "GET", f"/api/recordings/{drop['id']}/download", raw=True)
    check("未留存片段拒绝新下载(409)", status == 409, str(data))

    # draining 期间手动触发排空：应因在途下载而等待，数据不被清空
    status, _, data = req("POST", "/api/partitions/p1/drain")
    check("在途下载未结束时不清空", status == 200
          and data["action"] == "waiting" and data["active_downloads"] == 1,
          str(data))
    ov = overview()
    check("等待期间 p1 上录像文件仍在（used>0）", part(ov, "p1")["used"] > 0)

    # 等慢速下载读完原字节
    t.join(timeout=60)
    check("慢速下载读完原字节且内容完整",
          dl_result.get("ok") is True and dl_result.get("status") == 200
          and dl_result.get("partition") == "p1", str(dl_result))

    # 后台线程应自动把 p1 排空到 idle；再手动触发兜底
    p1_after = wait_state("p1", "idle", timeout=25)
    check("下载结束后 p1 自动清空并回到 idle 周转池",
          p1_after["state"] == "idle" and p1_after["used"] == 0
          and p1_after["active_downloads"] == 0, str(p1_after))

    # 再封一次区，验证 idle 分区可继续参与周转（轮转顺序按接口返回判断）
    status, _, data = req("POST", "/api/partitions/seal")
    check("第二次封区成功并提升新的周转分区",
          status == 200 and data["sealed"] == "p2"
          and data["new_active"] in ("p1", "p3"), str(data))
    rec_next = upload("on-next.bin", b"NEXT")
    check("新提升的周转分区可正常承接新录像",
          rec_next["current_partition"] == data["new_active"],
          f"{rec_next['current_partition']} != {data['new_active']}")


def test_move_failure():
    print("\n=== 场景3: 搬移写入失败 + 清理周转区后重试 ===")
    # 当前状态：p2=sealed（上一场景结尾），p3=active，p1=idle
    wait_state("p2", "sealed")
    # p2 上已有上一场景搬入的留存片段（source=p1, current=p2, retained=1）
    ov = overview()
    p2_retained = [r for r in ov["recordings"]
                   if r["current_partition"] == "p2" and r["retained"] == 1]
    check("p2 上存在待搬移的留存片段", len(p2_retained) >= 1,
          f"{[r['id'] for r in p2_retained]}")

    # 注入故障：写入 2000 字节后失败
    status, _, data = req("POST", "/api/test/fail-move",
                          {"after_bytes": 2000})
    check("故障注入成功", status == 200 and data["move_fail_after"] == 2000,
          str(data))

    status, _, data = req("POST", "/api/partitions/p2/recycle")
    check("搬移失败时接口明确返回 failed",
          status == 200 and data["status"] == "failed"
          and data["failed"][0]["error"], str(data))
    failed_rid = data["failed"][0]["rid"]

    ov = overview()
    p2 = part(ov, "p2")
    check("失败后 p2 仍为 recycling 且记录错误",
          p2["state"] == "recycling" and bool(p2["last_error"]))
    rec_failed = next(r for r in ov["recordings"] if r["id"] == failed_rid)
    check("失败片段归属仍在原分区 p2（原录像可用）",
          rec_failed["current_partition"] == "p2")
    status, headers, body = req(
        "GET", f"/api/recordings/{failed_rid}/download", raw=True)
    check("失败后原录像仍可正常下载（原目录照常可用）",
          status == 200 and len(body) == rec_failed["size"],
          f"status={status}")

    # 故障触发时回收流程会即时删除本次的 .part；先手工在隔离 staging
    # 目录里放一个残留，验证 cleanup 接口确实能按源分区扫出并清理
    ov = overview()
    active_now = next(p for p in ov["partitions"] if p["state"] == "active")
    import os as _os
    stale_dir = _os.path.join(active_now["mount_path"], "staging", "p2")
    _os.makedirs(stale_dir, exist_ok=True)
    with open(_os.path.join(stale_dir, "rec_stale.part"), "wb") as f:
        f.write(b"stale-half-written")
    status, _, data = req("POST", "/api/partitions/p2/cleanup")
    check("清理周转区成功（删掉残留 .part，系统回到可重试状态）",
          status == 200 and data["removed_parts"] == 1, str(data))
    check("清理后 staging 隔离目录已移除",
          not _os.path.exists(stale_dir))
    req("POST", "/api/test/reset-fail")
    status, _, data = req("POST", "/api/partitions/p2/recycle")
    check("重试回收成功进入 draining",
          status == 200 and data["status"] == "draining", str(data))
    check("重试包含此前失败的片段", failed_rid in data["moved"],
          str(data["moved"]))

    # p2 上未留存片段：回收后拒绝新下载（409）；若已被清空则表现为文件缺失(409)
    ov = overview()
    unmarked_p2 = [r for r in ov["recordings"]
                   if r["source_partition"] == "p2" and not r["retained"]]
    if unmarked_p2:
        status2, _, d2 = req(
            "GET", f"/api/recordings/{unmarked_p2[0]['id']}/download", raw=True)
        check("未留存片段在回收后不再接受新下载", status2 == 409, str(d2))

    # 无在途下载，应很快排空到 idle
    p2_after = wait_state("p2", "idle", timeout=25)
    check("p2 排空回 idle，周转链闭环",
          p2_after["state"] == "idle" and p2_after["used"] == 0, str(p2_after))


def _wait_active_dl(rid: str, timeout: float = 10.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if any(d["recording_id"] == rid for d in overview()["active_downloads"]):
            return
        time.sleep(0.2)
    raise AssertionError("活动下载未登记")


def wait_no_active(pid: str, timeout: float = 10.0) -> None:
    """等下载生成器 finally 完成 token 收尾（客户端读完到登记 finished 有短窗口）。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if part(overview(), pid)["active_downloads"] == 0:
            return
        time.sleep(0.1)
    raise AssertionError(f"分区 {pid} 活动下载计数未归零")


def _slow_get(rid: str, slow: float = 0.3):
    return req("GET", f"/api/recordings/{rid}/download?slow={slow}",
               raw=True, timeout=180)


def test_offline_reattach():
    """场景6a：离线准入 + 拔盘/错盘/损坏 + 原卷接回 + 核验并发。

    前置（承接场景3结尾）：p1=active，p2/p3=idle。
    结束：p1=idle（接回后回收排空），p2=active，p3=idle。
    """
    print("\n=== 场景4: 封区卷离线/拔盘/错装/损坏/原卷接回 ===")
    import threading

    mount_path_hint = part(overview(), "p1")["mount_path"]
    fs_shared = os.path.isdir(mount_path_hint)

    small_payload = b"OFFLINE-CLIP-" * 1000          # 14KB
    big_payload = os.urandom(4 * 1024 * 1024)        # 4MB
    keep_payload = b"OFFLINE-KEEP-" * 1000           # 13KB
    small = upload("off-small.bin", small_payload)
    big = upload("off-big.bin", big_payload)
    keep = upload("off-keep.bin", keep_payload, retained=True)

    status, _, data = req("POST", "/api/partitions/seal")
    check("离线场景：封 p1、提升 p2", status == 200
          and data["sealed"] == "p1" and data["new_active"] == "p2", str(data))
    wait_state("p2", "active")

    # ---- 交错1：下载先获准（在途），离线必须驳回 ----
    dl = {}
    def run_slow():
        try:
            s, h, b = _slow_get(big["id"], 0.3)
            dl["status"], dl["len"], dl["part"] = (
                s, len(b), h.get("X-Source-Partition"))
        except Exception as exc:  # noqa: BLE001
            dl["error"] = repr(exc)
    t = threading.Thread(target=run_slow)
    t.start()
    _wait_active_dl(big["id"])

    status, _, data = req("POST", "/api/partitions/p1/offline")
    check("有在途下载时离线被驳回(409)且说明交错原因",
          status == 409 and "在途下载" in data.get("error", ""), str(data))
    ov = overview()
    check("驳回后 p1 仍为 sealed", part(ov, "p1")["state"] == "sealed")
    t.join(timeout=60)
    check("被等待的慢速下载完整读完",
          dl.get("status") == 200 and dl.get("len") == len(big_payload)
          and dl.get("part") == "p1", str(dl))
    wait_no_active("p1")

    # ---- 无在途后离线成功，状态/持久标识落库 ----
    status, _, data = req("POST", "/api/partitions/p1/offline")
    if not check("无在途下载后离线成功", status == 200
                 and data.get("state") == "offline", str(data)):
        return  # 后续拔盘/错盘编排以离线成功为前提，避免状态级联污染
    ov = overview()
    p1 = part(ov, "p1")
    expected_vid = p1["volume_id"]
    check("卡片数据带卷持久标识且当前标识匹配",
          bool(expected_vid) and p1["volume_match"] is True
          and p1["mount_status"] == "ok", str(p1))

    # ---- 离线后拒绝新下载与回收；重复离线也拒绝 ----
    status, _, _ = req("GET", f"/api/recordings/{small['id']}/download", raw=True)
    check("离线后该卷新下载被拒绝(409)", status == 409)
    status, _, data = req("POST", "/api/partitions/p1/recycle")
    check("离线后回收被拒绝(409)", status == 409 and "离线" in data.get("error", ""),
          str(data))
    status, _, _ = req("POST", "/api/partitions/p1/offline")
    check("重复离线被拒绝(409)", status == 409)

    if not fs_shared:
        # 测试进程看不到服务的挂载目录（例如从宿主机打 Docker 端口）：
        # 拔盘/错盘/损坏无法模拟，但离线持久态、HTTP 接回与恢复仍可验收。
        skip("拔盘/空挂载点/错盘/损坏的目录模拟",
             f"测试进程访问不到 {mount_path_hint}")
        status, _, data = req("POST", "/api/partitions/p1/reattach")
        check("共享 FS 不可见时原卷仍在服务端挂载，接回成功",
              status == 200 and data.get("ok") is True, str(data)[:200])
        for rec, payload in ((small, small_payload), (big, big_payload),
                             (keep, keep_payload)):
            s, h, b = req("GET", f"/api/recordings/{rec['id']}/download", raw=True)
            check(f"接回后下载恢复且字节一致 {rec['id'][:14]}",
                  s == 200 and b == payload
                  and h.get("X-Source-Partition") == "p1", f"status={s}")
        status, _, data = req("POST", "/api/partitions/p1/recycle")
        check("接回后回收恢复", status == 200
              and data.get("status") == "draining", str(data)[:200])
        wait_state("p1", "idle", timeout=25)
        return

    mount = p1["mount_path"]
    bak = mount + ".e2e-orig"
    if os.path.exists(bak):
        shutil.rmtree(bak)

    # ---- 模拟拔盘：整个卷目录移走，挂载路径不存在 ----
    os.rename(mount, bak)
    try:
        status, _, data = req("POST", "/api/partitions/p1/reattach")
        rep = data.get("report", {})
        check("卷没装回(路径缺失)时接回被拒并保持离线",
              status == 409 and rep.get("reason") == "volume_absent", str(data))
        ov = overview()
        check("路径缺失时总览探测为 absent 且仍 offline",
              part(ov, "p1")["mount_status"] == "absent"
              and part(ov, "p1")["state"] == "offline")

        # entrypoint 重启前会 mkdir -p 空挂载点
        os.makedirs(mount)
        status, _, data = req("POST", "/api/partitions/p1/reattach")
        rep = data.get("report", {})
        check("空挂载点(无卷标识)被拒并保持离线",
              status == 409 and rep.get("reason") == "no_identity", str(data))

        # ---- 模拟错装：把 p3 卷（带不同持久标识）装到 p1 ----
        os.rmdir(mount)
        ov = overview()
        p3_mount = part(ov, "p3")["mount_path"]
        shutil.copytree(p3_mount, mount)
        status, _, data = req("POST", "/api/partitions/p1/reattach")
        rep = data.get("report", {})
        check("错装卷(持久标识不同)被判定 wrong_volume 并保持离线",
              status == 409 and rep.get("reason") == "wrong_volume"
              and rep.get("expected_volume_id") == expected_vid
              and rep.get("mounted_volume_id") != expected_vid, str(data)[:300])
        ov = overview()
        check("总览显示卷标识不匹配且仍 offline",
              part(ov, "p1")["volume_match"] is False
              and part(ov, "p1")["state"] == "offline")

        # ---- 原卷装回 ----
        shutil.rmtree(mount)
        os.rename(bak, mount)

        def p1_abs(rec):
            return os.path.join(mount, rec["relpath"])

        # ---- 核验失败1：登记文件缺失 ----
        missing_path = p1_abs(small)
        os.remove(missing_path)
        status, _, data = req("POST", "/api/partitions/p1/reattach")
        rep = data.get("report", {})
        check("文件缺失时核验失败并保持离线",
              status == 409 and rep.get("reason") == "verification_failed"
              and any("不存在" in f["error"] for f in rep.get("failures", [])),
              str(data)[:300])
        with open(missing_path, "wb") as f:
            f.write(small_payload)

        # ---- 核验失败2：大小不符 ----
        with open(p1_abs(small), "ab") as f:
            f.write(b"extra-bytes")
        status, _, data = req("POST", "/api/partitions/p1/reattach")
        rep = data.get("report", {})
        check("大小不符被检出并保持离线",
              status == 409
              and any("大小不符" in f["error"] for f in rep.get("failures", [])),
              str(data)[:300])
        with open(p1_abs(small), "wb") as f:
            f.write(small_payload)

        # ---- 核验失败3：同长度篡改 -> 摘要不符 ----
        big_path = p1_abs(big)
        with open(big_path, "r+b") as f:
            f.seek(37)
            f.write(bytes([big_payload[37] ^ 0xFF]))
        status, _, data = req("POST", "/api/partitions/p1/reattach")
        rep = data.get("report", {})
        check("字节篡改导致 sha256 摘要不符并保持离线",
              status == 409
              and any("摘要不符" in f["error"] for f in rep.get("failures", [])),
              str(data)[:300])
        with open(big_path, "wb") as f:
            f.write(big_payload)

        ov = overview()
        n_p1 = sum(1 for r in ov["recordings"]
                   if r["current_partition"] == "p1")

        # ---- 核验期间其他分区照常读写（测试钩子放大核验窗口）----
        hooks_on = bool(ov.get("failure"))
        final_report = None
        if hooks_on:
            req("POST", "/api/test/verify-delay", {"seconds": 0.4})
            verify_result = {}
            def do_reattach():
                s, _, d = req("POST", "/api/partitions/p1/reattach", timeout=120)
                verify_result["status"] = s
                verify_result["data"] = d
            vt = threading.Thread(target=do_reattach)
            vt.start()
            time.sleep(0.3)  # 等核验进入逐文件睡眠
            concurrent = upload("during-verify-p2.bin", b"WRITE-WHILE-VERIFY")
            cs, _, cb = req("GET",
                            f"/api/recordings/{concurrent['id']}/download", raw=True)
            t_ov = time.time()
            ov2 = overview()
            overview_ms = (time.time() - t_ov) * 1000
            vt.join(timeout=90)
            req("POST", "/api/test/verify-delay", {"seconds": 0})
            check("核验期间其他分区可正常写入",
                  concurrent["current_partition"] == "p2", str(concurrent))
            check("核验期间其他分区可正常读取且字节一致",
                  cs == 200 and cb == b"WRITE-WHILE-VERIFY", f"status={cs}")
            check("核验期间总览接口不被哈希阻塞(<1s)",
                  overview_ms < 1000, f"{overview_ms:.0f}ms")
            check("延迟核验最终成功",
                  verify_result.get("status") == 200
                  and verify_result["data"].get("ok") is True,
                  str(verify_result.get("data"))[:200])
            final_report = verify_result.get("data")
        else:
            skip("核验期间并发读写时序放大", "服务未启用 ENABLE_TEST_HOOKS")
            status, _, data = req("POST", "/api/partitions/p1/reattach")
            check("原卷接回核验成功",
                  status == 200 and data.get("ok") is True, str(data)[:200])
            final_report = data

        check("接回核验条数覆盖该卷全部登记录像",
              bool(final_report) and final_report.get("checked") == n_p1,
              f"checked={final_report and final_report.get('checked')} n={n_p1}")

        # 已接回为 sealed：再次接回应被拒绝，状态不被改写
        status, _, data = req("POST", "/api/partitions/p1/reattach")
        check("接回后重复接回被拒（已是 sealed）",
              status == 409 and "不是离线状态" in data.get("error", ""), str(data)[:160])

        ov = overview()
        check("接回后 p1 为 sealed、卷匹配且记录核验时间",
              part(ov, "p1")["state"] == "sealed"
              and part(ov, "p1")["volume_match"] is True
              and bool(part(ov, "p1")["verified_at"]))

        # ---- 原下载链接恢复：本场景三个片段字节一致 ----
        for rec, payload in ((small, small_payload), (big, big_payload),
                             (keep, keep_payload)):
            s, h, b = req("GET", f"/api/recordings/{rec['id']}/download", raw=True)
            check(f"接回后原链接恢复下载且字节一致 {rec['id'][:14]}",
                  s == 200 and b == payload
                  and h.get("X-Source-Partition") == "p1", f"status={s}")

        # ---- 回收能力恢复：留存片段可搬移，无在途后自动排空 ----
        status, _, data = req("POST", "/api/partitions/p1/recycle")
        check("接回后回收恢复并进入 draining",
              status == 200 and data.get("status") == "draining", str(data)[:200])
        p1_after = wait_state("p1", "idle", timeout=25)
        check("回收闭环：p1 排空回 idle", p1_after["state"] == "idle", str(p1_after))
    finally:
        # 无论断言如何，尽量把原卷放回，避免遗留目录影响后续场景
        if not os.path.isdir(mount) and os.path.isdir(bak):
            os.rename(bak, mount)
        elif os.path.isdir(bak):
            shutil.rmtree(bak, ignore_errors=True)


def test_offline_race_and_recycle_first():
    """场景6b：下载/离线真并发竞态不变量 + 回收先获准驳回离线。

    前置（承接场景4）：p2=active，p1/p3=idle。
    结束：p2=idle，p1=active，p3=idle。
    """
    print("\n=== 场景5: 请求交错竞态（下载/回收先获准则离线驳回）===")
    import threading

    race_payload = os.urandom(8 * 1024 * 1024)
    retained_payload = b"RACE-RETAINED" * 100
    race_rec = upload("race-big.bin", race_payload)
    retained_rec = upload("race-keep.bin", retained_payload, retained=True)

    status, _, data = req("POST", "/api/partitions/seal")
    check("竞态场景：封 p2、提升 p1", status == 200
          and data["sealed"] == "p2" and data["new_active"] == "p1", str(data))
    wait_state("p1", "active")

    # ---- 真并发：一个慢下载与离线同时发出，断言唯一赢家不变量 ----
    barrier = threading.Barrier(2, timeout=15)
    race = {}

    def race_download():
        try:
            barrier.wait()
            s, h, b = req(
                "GET", f"/api/recordings/{race_rec['id']}/download?slow=0.2",
                raw=True, timeout=180)
            race["dl_status"] = s
            race["dl_len"] = len(b)
        except Exception as exc:  # noqa: BLE001
            race["dl_error"] = repr(exc)

    def race_offline():
        try:
            barrier.wait()
            s, _, d = req("POST", "/api/partitions/p2/offline")
            race["off_status"] = s
            race["off_data"] = d
        except Exception as exc:  # noqa: BLE001
            race["off_error"] = repr(exc)

    td = threading.Thread(target=race_download)
    to = threading.Thread(target=race_offline)
    td.start(); to.start()
    to.join(timeout=30)

    off_status = race.get("off_status")
    if off_status == 200:
        # 离线先获准：下载必须在准入临界区被拒（不可能存在活动 token）
        td.join(timeout=60)
        check("竞态不变量：离线先获准 -> 并发下载被 409 驳回",
              race.get("dl_status") == 409, str(race))
        ov = overview()
        check("竞态不变量：离线成功时 p2 无活动下载",
              part(ov, "p2")["state"] == "offline"
              and part(ov, "p2")["active_downloads"] == 0)
        status, _, data = req("POST", "/api/partitions/p2/reattach")
        check("竞态后原卷接回恢复 sealed",
              status == 200 and data.get("state") == "sealed", str(data)[:200])
    else:
        # 下载先获准：离线必须以“在途下载”驳回；下载须完整成功
        check("竞态不变量：下载先获准 -> 离线 409 且理由为在途下载",
              off_status == 409
              and "在途下载" in race.get("off_data", {}).get("error", ""),
              str(race.get("off_data")))
        td.join(timeout=90)
        check("竞态不变量：先获准的下载完整读完",
              race.get("dl_status") == 200
              and race.get("dl_len") == len(race_payload), str(race)[:200])
        wait_no_active("p2")
        status, _, _ = req("POST", "/api/partitions/p2/offline")
        if not check("下载结束后离线可成功", status == 200):
            return
        status, _, data = req("POST", "/api/partitions/p2/reattach")
        check("随后原卷接回恢复 sealed",
              status == 200 and data.get("state") == "sealed", str(data)[:200])

    # ---- 回收先获准：注入失败让回收停在 recycling，离线必须被驳回 ----
    status, _, _ = req("POST", "/api/test/fail-move", {"after_bytes": 1})
    if status != 200:
        skip("回收优先交错（故障钩子未启用）", f"fail-move status={status}")
    else:
        status, _, data = req("POST", "/api/partitions/p2/recycle")
        check("故障注入下回收已获准并停在 failed/recycling",
              status == 200 and data.get("status") == "failed", str(data)[:200])
        status, _, data = req("POST", "/api/partitions/p2/offline")
        check("回收先获准(回收中) -> 离线被驳回(409)",
              status == 409 and "回收已获准" in data.get("error", ""), str(data))
        req("POST", "/api/test/reset-fail")
        status, _, data = req("POST", "/api/partitions/p2/cleanup")
        check("回收失败后清理成功", status == 200, str(data))
        status, _, data = req("POST", "/api/partitions/p2/recycle")
        check("清理后重试回收进入 draining",
              status == 200 and data.get("status") == "draining", str(data)[:200])
        check("留存片段确实搬移成功", retained_rec["id"] in data.get("moved", []),
              str(data.get("moved")))
        wait_state("p2", "idle", timeout=25)
        check("竞态场景收尾：p2 排空回 idle",
              part(overview(), "p2")["state"] == "idle")


def main():
    print(f"BASE_URL={BASE}")
    reset_world()
    test_basic_flow()
    test_recycle_during_download()
    test_move_failure()
    test_offline_reattach()
    test_offline_race_and_recycle_first()
    print("\n================ 汇总 ================")
    for tag, name, detail in RESULTS:
        print(f"{tag:4} {name}" + (f"  ({detail})" if detail and tag == "FAIL" else ""))
    print(f"通过 {PASS} / 失败 {FAIL} / 跳过 {SKIP}")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
