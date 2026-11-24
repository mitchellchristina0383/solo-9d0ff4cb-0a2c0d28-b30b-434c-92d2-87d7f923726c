# -*- coding: utf-8 -*-
"""离线卷“重启保持 + 重启后错盘 + 原卷接回”的进程级验收。

与 e2e_test.py 不同，本脚本自己拉起/重启 `run.py` 子进程，使用临时
DATA_DIR 的目录模式，通过目录改名/复制模拟拔盘与错装，因此可以验证：

  1. offline 状态跨进程重启保持（SQLite 持久化）；
  2. 拔盘后重启：entrypoint 重建空挂载点，启动引导不污染离线卷；
  3. 重启后装错卷（别的分区目录，持久标识不同）：接回被拒、继续离线；
  4. 错盘状态下再次重启，仍然保持离线且仍然识别为错盘；
  5. 原卷装回：标识 + 每条录像大小/摘要核验通过，恢复 sealed 与下载；
  6. 接回后回收能力恢复。

用法：
    python tests/offline_restart_e2e.py
（会自动选用可用的 python 解释器；也可通过 PYTHON 环境变量指定带 Flask 的解释器）
"""
from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON = os.environ.get("PYTHON", sys.executable)
PASS = FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"[PASS] {name}", flush=True)
    else:
        FAIL += 1
        print(f"[FAIL] {name} -- {detail}", flush=True)


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def http(method, port, path, body=None, timeout=30):
    url = f"http://127.0.0.1:{port}{path}"
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    r = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, dict(resp.headers), json.loads(raw or b"{}")
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, dict(e.headers), json.loads(raw)
        except json.JSONDecodeError:
            return e.code, dict(e.headers), {"_raw": raw.decode(errors="replace")}


def upload(port, filename, payload):
    boundary = "----restart" + uuid.uuid4().hex
    parts = []
    parts.append(
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"size\"\r\n\r\n"
        f"{len(payload)}\r\n".encode())
    parts.append(
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; "
        f"filename=\"{filename}\"\r\nContent-Type: application/octet-stream\r\n\r\n"
        .encode())
    parts.append(payload)
    parts.append(f"\r\n--{boundary}--\r\n".encode())
    body = b"".join(parts)
    r = urllib.request.Request(
        f"http://127.0.0.1:{port}/api/recordings", data=body, method="POST",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
    with urllib.request.urlopen(r, timeout=30) as resp:
        return json.loads(resp.read())


def wait_health(port, proc, log_path, timeout=40):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        if proc.poll() is not None:
            tail = _tail(log_path)
            raise RuntimeError(f"服务进程提前退出 code={proc.returncode}\n{tail}")
        try:
            with urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/api/overview", timeout=2) as r:
                if r.status == 200:
                    return
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(0.4)
    raise RuntimeError(f"服务在 {timeout}s 内未就绪: {last}\n{_tail(log_path)}")


def _tail(path, n=3000):
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - n))
            return f.read().decode(errors="replace")
    except OSError:
        return "(no log)"


def start_server(data_dir, port, log_path):
    env = os.environ.copy()
    env.update({
        "DATA_DIR": data_dir,
        "DB_PATH": os.path.join(data_dir, "meta.db"),
        "HOST": "127.0.0.1",
        "PORT": str(port),
        "ENABLE_TEST_HOOKS": "1",
        "DRAIN_POLL_INTERVAL": "1",
        "PYTHONUNBUFFERED": "1",
    })
    log = open(log_path, "ab")
    proc = subprocess.Popen(
        [PYTHON, os.path.join(ROOT, "run.py")],
        cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
    wait_health(port, proc, log_path)
    return proc


def stop_server(proc):
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def overview(port):
    _, _, ov = http("GET", port, "/api/overview")
    return ov


def part(ov, pid):
    return next(p for p in ov["partitions"] if p["id"] == pid)


def main():
    data_dir = tempfile.mkdtemp(prefix="er-restart-")
    log_path = os.path.join(data_dir, "server.log")
    port = free_port()
    print(f"DATA_DIR={data_dir}\nPORT={port}\nPYTHON={PYTHON}", flush=True)

    proc = start_server(data_dir, port, log_path)
    try:
        payloads = {
            "a.bin": b"RESTART-CLIP-A-" * 500,
            "b.bin": os.urandom(2 * 1024 * 1024),
        }
        recs = []
        for name, payload in payloads.items():
            rec = upload(port, name, payload)
            check(f"上传 {name} 成功落 p1", rec["current_partition"] == "p1")
            recs.append(rec)
        # 标记一个留存，供接回后验证回收
        s, _, _ = http("POST", port,
                       f"/api/recordings/{recs[0]['id']}/retained",
                       {"retained": True})
        check("标记留存成功", s == 200)

        s, _, d = http("POST", port, "/api/partitions/seal")
        check("封区 p1 并提升 p2", s == 200 and d.get("sealed") == "p1", str(d))

        s, _, d = http("POST", port, "/api/partitions/p1/offline")
        check("首次离线 p1 成功", s == 200 and d.get("state") == "offline", str(d))
        vid = part(overview(port), "p1")["volume_id"]
        check("离线时已登记持久卷标识", bool(vid))

        # ---- 模拟拔盘 + 真实重启（entrypoint 会重建空挂载点）----
        p1 = os.path.join(data_dir, "p1")
        orig = os.path.join(data_dir, "p1.original-volume")
        os.rename(p1, orig)
        os.makedirs(p1)  # 等价 10-partitions.sh 的 mkdir -p $DATA_DIR/p1

        stop_server(proc)
        print("-- 已拔盘并停止服务，重新启动进程 --", flush=True)
        proc = start_server(data_dir, port, log_path)

        p = part(overview(port), "p1")
        check("重启后 offline 状态保持", p["state"] == "offline", str(p["state"]))
        check("重启后原卷标识仍在元数据中", p["volume_id"] == vid)
        check("空挂载点被探测为 no_identity（引导未污染离线卷）",
              p["mount_status"] == "no_identity", p["mount_status"])
        check("空挂载点里没有被 bootstrap 重建录像目录",
              not os.path.isdir(os.path.join(p1, "recordings")))

        s, _, d = http("POST", port, "/api/partitions/p1/reattach")
        rep = d.get("report", {})
        check("重启后卷未装回：接回被拒(no_identity)且保持离线",
              s == 409 and rep.get("reason") == "no_identity"
              and part(overview(port), "p1")["state"] == "offline", str(d)[:200])

        # ---- 装错盘：复制 p3（带不同持久标识）----
        p3 = os.path.join(data_dir, "p3")
        wrong = os.path.join(data_dir, "p1.wrong-volume")
        if os.path.exists(wrong):
            shutil.rmtree(wrong)
        os.rename(p1, wrong)          # 移走空挂载点目录
        shutil.copytree(p3, p1)       # 错装：p3 的卷
        s, _, d = http("POST", port, "/api/partitions/p1/reattach")
        rep = d.get("report", {})
        check("重启后错盘：持久标识不符 wrong_volume 且保持离线",
              s == 409 and rep.get("reason") == "wrong_volume"
              and rep.get("expected_volume_id") == vid
              and rep.get("mounted_volume_id") != vid, str(d)[:220])

        # ---- 错盘状态下再次重启：离线与错盘判定都要保持 ----
        stop_server(proc)
        print("-- 错盘在位，再次重启 --", flush=True)
        proc = start_server(data_dir, port, log_path)
        p = part(overview(port), "p1")
        check("错盘+重启后仍为 offline", p["state"] == "offline")
        check("错盘+重启后仍识别 volume_match=False", p["volume_match"] is False
              and p["mount_status"] == "ok")
        s, _, d = http("POST", port, "/api/partitions/p1/reattach")
        check("错盘+重启后接回仍被驳回",
              s == 409 and d.get("report", {}).get("reason") == "wrong_volume")

        # ---- 原卷接回 ----
        shutil.rmtree(p1)
        os.rename(orig, p1)
        s, _, d = http("POST", port, "/api/partitions/p1/reattach", timeout=120)
        check("原卷接回核验通过",
              s == 200 and d.get("ok") is True and d.get("checked") == len(recs),
              str(d)[:260])
        check("核验字节总数等于两条录像大小之和",
              d.get("total_bytes") == sum(len(x) for x in payloads.values()),
              str(d.get("total_bytes")))
        p = part(overview(port), "p1")
        check("接回后状态恢复 sealed 且卷匹配",
              p["state"] == "sealed" and p["volume_match"] is True)

        # ---- 原下载链接恢复，字节一致 ----
        for rec in recs:
            req = urllib.request.Request(
                f"http://127.0.0.1:{port}/api/recordings/{rec['id']}/download")
            with urllib.request.urlopen(req, timeout=30) as r:
                body = r.read()
                src = r.headers.get("X-Source-Partition")
            check(f"接回后下载 {rec['id'][:14]} 字节一致且物理分区=p1",
                  r.status == 200 and body == payloads[rec["filename"]]
                  and src == "p1", f"len={len(body)} src={src}")

        # ---- 回收恢复 ----
        s, _, d = http("POST", port, "/api/partitions/p1/recycle")
        check("接回后回收获准并进入 draining",
              s == 200 and d.get("status") == "draining", str(d)[:200])
        deadline = time.time() + 25
        state = None
        while time.time() < deadline:
            state = part(overview(port), "p1")["state"]
            if state == "idle":
                break
            time.sleep(0.5)
        check("回收完成，p1 排空回 idle", state == "idle", str(state))

        print(f"\n临时数据目录保留在: {data_dir}")
        print(f"服务日志: {log_path}")
        print(f"================ 重启验收: 通过 {PASS} / 失败 {FAIL} ================")
        return 1 if FAIL else 0
    finally:
        stop_server(proc)


if __name__ == "__main__":
    sys.exit(main())
