#!/usr/bin/env python3
"""并发导入可控演示：双 API 共库下的整批导入正确性。

对运行中的两个共库 API 进程（默认 8001/8002）演示：

  1. 双方通过检查后交错写入：用 demo_check_delay_seconds 撑大"检查→写入"
     窗口，两批同键异值必然交错 —— 结果只能一批 201，另一批 409 并指出
     冲突版本（含已存值与写入它的批次）；失败批的独有行一律不落库；
     登记（/api/v1/imports）与实际写入一致。
  2. 反序重叠：两批包含相同两个事件但顺序相反 —— 正常判定（201+409），
     不死锁、不挂起、不 500。
  3. 一批等待重叠事件时，无关事件导入不被阻塞（实测延迟远小于演示窗口）。
  4. 同值重传幂等：原样重发返回 201 且 imported=0；两批相同负载并发时
     双双 201（一个写入、一个幂等），行只落一次。
  5. 回归：时点计算、固化栅栏、迟到更正隔离与 keyset 分页入口不受影响。

脚本使用本次运行专属的事件/申请 ID，可重复执行；任何断言失败即非零退出。

用法: python3 scripts/race_demo.py [BASE_URL_1] [BASE_URL_2]
"""
from __future__ import annotations

import json
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid

BASE1 = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8001"
BASE2 = sys.argv[2] if len(sys.argv) > 2 else "http://127.0.0.1:8002"
RUN = uuid.uuid4().hex[:8]
DELAY = 2.5  # seconds; widens the check->write window so the race is certain


def call(base: str, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        base + path, data=data,
        headers={"Content-Type": "application/json"}, method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def heading(title: str) -> None:
    print("\n" + "=" * 72 + f"\n{title}\n" + "=" * 72)


def ev(event_id: str, income: int | None, customer: str = "RC",
       known: str = "2026-01-02T00:00:00Z", biz: str = "2026-01-01T00:00:00Z",
       withdrawn: bool = False) -> dict:
    return {"event_id": event_id, "customer_id": customer, "business_time": biz,
            "known_time": known, "income": income, "withdrawn": withdrawn}


def post_events(base: str, events: list[dict], delay: float = 0.0) -> tuple[int, dict]:
    q = f"?demo_check_delay_seconds={delay}" if delay else ""
    return call(base, "POST", f"/api/v1/imports/credit-events{q}", {"events": events})


def fire_concurrently(jobs: list[tuple[str, list[dict], float]]) -> list[tuple[int, dict, float]]:
    """POST several imports at the same instant; return (status, body, seconds)."""
    barrier = threading.Barrier(len(jobs) + 1)
    results: list[tuple[int, dict, float] | None] = [None] * len(jobs)

    def worker(i: int, base: str, events: list[dict], delay: float) -> None:
        barrier.wait()
        t0 = time.monotonic()
        code, body = post_events(base, events, delay)
        results[i] = (code, body, time.monotonic() - t0)

    threads = [threading.Thread(target=worker, args=(i, *job))
               for i, job in enumerate(jobs)]
    for t in threads:
        t.start()
    barrier.wait()
    for t in threads:
        t.join()
    return [r for r in results if r is not None]


def stored_events() -> dict[str, dict]:
    _, rows = call(BASE1, "GET", "/api/v1/events")
    return {r["event_id"]: r for r in rows}


def ledger() -> dict[str, dict]:
    _, rows = call(BASE1, "GET", "/api/v1/imports")
    return {r["ingest_batch_id"]: r for r in rows}


def wait_completed(batch_id: str, timeout: float = 90) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, batch = call(BASE2, "GET", f"/api/v1/batches/{batch_id}")
        if batch["status"] in ("COMPLETED", "FAILED"):
            return batch
        time.sleep(0.2)
    raise TimeoutError(f"batch {batch_id} did not finish")


def fetch_all_results(batch_id: str, limit: int = 2, expect_paged: bool = False) -> list[dict]:
    items, cursor, pages = [], None, 0
    while True:
        path = f"/api/v1/batches/{batch_id}/results?limit={limit}"
        if cursor:
            path += f"&cursor={cursor}"
        code, page = call(BASE1, "GET", path)
        assert code == 200, (code, page)
        items.extend(page["items"])
        pages += 1
        cursor = page["next_cursor"]
        if not cursor:
            break
    if expect_paged:
        assert pages > 1, "expected the demo to exercise keyset pagination"
    return items


def main() -> None:
    heading(f"0. 两个共库 API 进程健康检查 (run={RUN})")
    for base in (BASE1, BASE2):
        code, body = call(base, "GET", "/health")
        assert code == 200, (base, code, body)
        print(base, "->", body)

    # ------------------------------------------------------------------ #
    heading("1. 双方通过检查后交错写入 -> 只能一批成功，失败批整批不落库")
    s, ua, ub = f"T1-S-{RUN}", f"T1-A-{RUN}", f"T1-B-{RUN}"
    jobs = [
        (BASE1, [ev(s, 1000), ev(ua, 1111)], DELAY),
        (BASE2, [ev(s, 2000), ev(ub, 2222)], DELAY),
    ]
    (ra, rb) = fire_concurrently(jobs)
    codes = sorted([ra[0], rb[0]])
    print(f"status codes: {[ra[0], rb[0]]} (elapsed {ra[2]:.2f}s/{rb[2]:.2f}s)")
    assert codes == [201, 409], f"expected exactly one winner, got {codes}"
    win, lose = (ra, rb) if ra[0] == 201 else (rb, ra)
    win_income = 1000 if win is ra else 2000
    win_uniq, lose_uniq = (ua, ub) if win is ra else (ub, ua)

    # 409 指出冲突版本：已存值、写入它的批次、被拒值
    conflict = lose[1]["conflicts"][0]
    assert conflict["event_id"] == s, conflict
    assert conflict["stored"]["income"] == win_income, conflict
    assert conflict["stored"]["ingest_batch_id"] == win[1]["ingest_batch_id"], conflict
    assert conflict["rejected"]["income"] == (2000 if win_income == 1000 else 1000)
    print(f"409 指出冲突版本: {s} stored_income={conflict['stored']['income']} "
          f"written_by={conflict['stored']['ingest_batch_id'][:8]}… ✔")

    stored = stored_events()
    assert stored[s]["income"] == win_income and stored[s]["ingest_batch_id"] == win[1]["ingest_batch_id"]
    assert win_uniq in stored, "winner's unique row must be stored"
    assert lose_uniq not in stored, "loser's unique row must NOT be stored (整批拒绝)"
    print(f"落库核验: {s}={stored[s]['income']}（胜方版本），胜方独有行 {win_uniq} 在库，"
          f"负方独有行 {lose_uniq} 未落库 ✔")

    books = ledger()
    assert books[win[1]["ingest_batch_id"]]["status"] == "accepted"
    assert books[lose[1]["ingest_batch_id"]]["status"] == "rejected"
    assert books[lose[1]["ingest_batch_id"]]["reason"].startswith("conflicts with stored versions")
    print("登记核验: 胜方 accepted / 负方 rejected，与实际写入一致 ✔")

    # ------------------------------------------------------------------ #
    heading("2. 反序重叠：两批相同事件、顺序相反 -> 正常判定，无死锁/500")
    e1, e2 = f"T2-1-{RUN}", f"T2-2-{RUN}"
    jobs = [
        (BASE1, [ev(e1, 10), ev(e2, 10)], DELAY),   # 顺序 E1,E2
        (BASE2, [ev(e2, 20), ev(e1, 20)], DELAY),   # 反序 E2,E1
    ]
    t0 = time.monotonic()
    (rc, rd) = fire_concurrently(jobs)
    elapsed = time.monotonic() - t0
    codes = sorted([rc[0], rd[0]])
    print(f"status codes: {[rc[0], rd[0]]}, total {elapsed:.2f}s (窗口 {DELAY}s)")
    assert codes == [201, 409], f"reverse-order overlap must yield 201+409, got {codes}"
    assert elapsed < DELAY + 20, f"took too long ({elapsed:.1f}s) -- 互等挂起?"
    lose2 = rc if rc[0] == 409 else rd
    named = {c["event_id"] for c in lose2[1]["conflicts"]}
    assert named == {e1, e2}, named
    stored = stored_events()
    win2 = rc if rc[0] == 201 else rd
    assert stored[e1]["ingest_batch_id"] == win2[1]["ingest_batch_id"]
    assert stored[e2]["ingest_batch_id"] == win2[1]["ingest_batch_id"]
    print(f"两事件均判给胜方，409 列出全部冲突键 {sorted(named)}，无 500/挂起 ✔")

    # ------------------------------------------------------------------ #
    heading("3. 一批等待重叠事件时，无关事件仍可导入（不被阻塞）")
    e3, u3 = f"T3-S-{RUN}", f"T3-U-{RUN}"
    jobs = [
        (BASE1, [ev(e3, 1)], DELAY + 1.0),
        (BASE2, [ev(e3, 2)], DELAY + 1.0),
    ]
    barrier = threading.Barrier(3)
    slow: list[tuple[int, dict, float] | None] = [None, None]

    def slow_worker(i: int, base: str, events: list[dict], delay: float) -> None:
        barrier.wait()
        t = time.monotonic()
        code, body = post_events(base, events, delay)
        slow[i] = (code, body, time.monotonic() - t)

    threads = [threading.Thread(target=slow_worker, args=(i, *job))
               for i, job in enumerate(jobs)]
    for t in threads:
        t.start()
    barrier.wait()
    time.sleep(0.8)  # 两批都已进入"检查后等待"窗口
    t0 = time.monotonic()
    g_code, g_body = post_events(BASE1, [ev(u3, 9)])
    g_elapsed = time.monotonic() - t0
    for t in threads:
        t.join()
    print(f"无关事件导入: {g_code} in {g_elapsed:.2f}s（重叠批仍在窗口内）")
    assert g_code == 201 and g_body["imported"] == 1, (g_code, g_body)
    assert g_elapsed < DELAY, f"unrelated import was blocked: {g_elapsed:.2f}s"
    slow_codes = sorted([slow[0][0], slow[1][0]])  # type: ignore[index]
    assert slow_codes == [201, 409], slow_codes
    print(f"无关事件 {u3} 立即成功（{g_elapsed:.2f}s），重叠批随后判出 {slow_codes} ✔")

    # ------------------------------------------------------------------ #
    heading("4. 同值重传幂等")
    # 4a. 原样重发第 1 阶段胜方负载 -> 201 imported=0
    code, body = post_events(BASE1, [ev(s, win_income), ev(win_uniq, 1111 if win_uniq == ua else 2222)])
    assert code == 201 and body["imported"] == 0, (code, body)
    print(f"原样重发胜方负载: 201 imported=0 ✔ (ingest {body['ingest_batch_id'][:8]}…)")
    # 4b. 两批相同负载并发 -> 双双 201，行只落一次
    i1 = f"T4-I-{RUN}"
    (p4, q4) = fire_concurrently([(BASE1, [ev(i1, 555)], DELAY),
                                  (BASE2, [ev(i1, 555)], DELAY)])
    assert (p4[0], q4[0]) == (201, 201), (p4[0], q4[0])
    imported = sorted([p4[1]["imported"], q4[1]["imported"]])
    assert imported == [0, 1], imported
    assert stored_events()[i1]["income"] == 555
    print(f"并发相同负载: 双双 201（imported {imported}），{i1} 仅落库一次 ✔")

    # ------------------------------------------------------------------ #
    heading("5. 回归：时点计算 / 固化栅栏 / 迟到更正隔离 / keyset 分页")
    pa, pb, pc = f"PA-{RUN}", f"PB-{RUN}", f"PC-{RUN}"
    pe1, pe2, pe3 = f"PE1-{RUN}", f"PE2-{RUN}", f"PE3-{RUN}"
    c1, c2 = f"PC1-{RUN}", f"PC2-{RUN}"
    code, _ = call(BASE1, "POST", "/api/v1/imports/applications", {"applications": [
        {"application_id": pa, "customer_id": c1, "application_time": "2026-01-11T12:00:00Z"},
        {"application_id": pb, "customer_id": c2, "application_time": "2026-02-20T00:00:00Z"},
        {"application_id": pc, "customer_id": c1, "application_time": "2026-02-20T00:00:00Z"},
    ]})
    assert code == 201, code
    code, _ = post_events(BASE1, [
        ev(pe1, 50000, c1, known="2026-01-02T09:00:00Z"),
        ev(pe1, 52000, c1, known="2026-01-12T09:00:00Z"),   # 修订晚于 PA 申请时刻
        ev(pe2, 60000, c1, known="2026-02-16T10:00:00Z", biz="2026-02-15T10:00:00Z"),
        ev(pe3, 40000, c2, known="2026-01-06T00:00:00Z", biz="2026-01-05T00:00:00Z"),
        ev(pe3, None, c2, known="2026-02-10T00:00:00Z", biz="2026-01-05T00:00:00Z", withdrawn=True),
    ])
    assert code == 201, code

    code, b1 = call(BASE1, "POST", "/api/v1/batches",
                    {"application_ids": [pa, pb, pc]})
    assert code == 201, (code, b1)
    final = wait_completed(b1["batch_id"])
    assert final["status"] == "COMPLETED", final
    rows = fetch_all_results(b1["batch_id"], limit=2, expect_paged=True)
    assert [r["application_id"] for r in rows] == sorted([pa, pb, pc]), rows
    by_app = {r["application_id"]: r for r in rows}
    assert (by_app[pa]["income"], by_app[pa]["source_event_id"]) == (50000, pe1), by_app[pa]
    assert by_app[pb]["income"] is None and by_app[pb]["basis"] == "no_known_event", by_app[pb]
    assert (by_app[pc]["income"], by_app[pc]["source_event_id"]) == (60000, pe2), by_app[pc]
    print("时点结果: PA=50000(修订前) PB=空(已撤回) PC=60000(最近业务时间)，分页遍历 ✔")

    # 迟到更正：known_time 在 PA 申请之前，但受理晚于批次固化 -> 旧批次不变
    code, _ = post_events(BASE2, [ev(pe1, 51000, c1, known="2026-01-08T09:00:00Z")])
    assert code == 201, code
    again = fetch_all_results(b1["batch_id"], limit=2)
    assert again == rows, "已完成批次被迟到更正改变!"
    code, b2 = call(BASE1, "POST", "/api/v1/batches", {"application_ids": [pa]})
    wait_completed(b2["batch_id"])
    rows2 = fetch_all_results(b2["batch_id"], limit=5)
    assert rows2[0]["income"] == 51000, rows2
    print("迟到更正隔离: 旧批次结果不变，新批次看到 51000 ✔")

    print("\nRACE DEMO FINISHED OK")


if __name__ == "__main__":
    main()
