"""FastAPI application: imports, immutable batch generation, paged results.

Run two processes against the same DATABASE_URL (see docker-compose.yml);
each serves the API and runs one cooperating worker.
"""
from __future__ import annotations

import asyncio
import base64
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime

import asyncpg
from fastapi import FastAPI, HTTPException, Query, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from . import config
from .db import close_pool, get_pool, init_pool, init_schema
from .models import (
    ApplicationsImport,
    BatchCreateRequest,
    BatchResponse,
    CreditEventsImport,
    ImportAccepted,
    RejectedResponse,
    ResultsPage,
    ResultRow,
)
from .worker import Worker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("income.api")

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _worker
    pool = await init_pool()
    await init_schema()
    _worker = Worker(pool, config.WORKER_ID)
    _worker.start()
    logger.info("api process up; worker id = %s", config.WORKER_ID)
    try:
        yield
    finally:
        await _worker.stop()
        await close_pool()


app = FastAPI(
    title="Point-in-time Income Backfill API",
    version="1.0.0",
    lifespan=lifespan,
    description=(
        "历史贷款申请的申请时点收入回填服务。征信事件按获知时间保留全部版本，"
        "生成批次固化受理时已提交的输入，两个 API 进程共享数据库并协作续算。"
    ),
)
_worker: Worker | None = None


@app.get("/health", tags=["ops"])
async def health() -> dict:
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.fetchval("SELECT 1")
    return {"status": "ok", "worker_id": config.WORKER_ID}


# --------------------------------------------------------------------------- #
# Imports
# --------------------------------------------------------------------------- #
# Concurrency contract (two API processes, one shared database):
#
#   * The primary key IS the arbiter. Version rows are written with a plain
#     INSERT (never ON CONFLICT DO NOTHING), so a version that lost a
#     concurrent race can never be silently swallowed while its batch is
#     registered as accepted.
#   * Canonical write order. Every importer inserts its rows ordered by the
#     natural key, so overlapping batches acquire row/unique-index locks in
#     the same global sequence and cannot deadlock -- even when two payloads
#     list the same events in opposite order.
#   * Rollback + re-check on collision. A unique violation aborts the whole
#     attempt (no partial rows, no ledger entry), then the batch is re-checked
#     against the freshly committed state: a differing stored version rejects
#     the entire batch with 409; an identical one is an idempotent success.
#   * Ledger == writes. "accepted" is recorded in the same transaction as the
#     data rows; "rejected" is recorded in a transaction that writes no data
#     rows; a rolled-back attempt leaves no ledger entry at all.
#   * No global locks. Only the touched keys serialize (via the unique
#     index), so imports of unrelated events never wait on each other.
async def _record_import(conn, kind: str, row_count: int, ok: bool, reason: str | None) -> uuid.UUID:
    ingest_id = uuid.uuid4()
    await conn.execute(
        """INSERT INTO import_batches
               (ingest_batch_id, import_kind, row_count, status, reason)
           VALUES ($1, $2, $3, $4, $5)""",
        ingest_id, kind, row_count, "accepted" if ok else "rejected", reason,
    )
    return ingest_id


def _reject_409(ingest_id: uuid.UUID, detail: str, conflicts: list[dict]) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content=jsonable_encoder(
            {"detail": detail, "ingest_batch_id": ingest_id, "conflicts": conflicts}
        ),
    )


@app.post(
    "/api/v1/imports/applications",
    response_model=ImportAccepted,
    status_code=status.HTTP_201_CREATED,
    tags=["imports"],
    summary="导入历史贷款申请（整批事务，失败全拒绝）",
    responses={409: {"model": RejectedResponse, "description": "整批拒绝：负载内重复或申请已存在"}},
)
async def import_applications(payload: ApplicationsImport) -> ImportAccepted:
    apps = payload.applications
    # Duplicates inside the payload.
    seen: set[str] = set()
    dupes = sorted({a.application_id for a in apps
                    if a.application_id in seen or seen.add(a.application_id)})

    pool = get_pool()
    for _attempt in range(config.IMPORT_MAX_ATTEMPTS):
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    if dupes:
                        ingest_id = await _record_import(
                            conn, "applications", len(apps), False,
                            f"duplicate application_id in payload: {dupes}",
                        )
                        return _reject_409(
                            ingest_id,
                            "whole import rejected: duplicate application_id in payload",
                            [{"application_id": i, "reason": "duplicated in payload"}
                             for i in dupes],
                        )

                    existing = await conn.fetch(
                        """SELECT application_id, ingested_at, ingest_batch_id
                             FROM applications WHERE application_id = ANY($1)""",
                        [a.application_id for a in apps],
                    )
                    if existing:
                        ids = [r["application_id"] for r in existing]
                        ingest_id = await _record_import(
                            conn, "applications", len(apps), False,
                            f"application_id already imported: {ids}",
                        )
                        return _reject_409(
                            ingest_id,
                            "whole import rejected: application_id already exists",
                            [{"application_id": r["application_id"],
                              "reason": "application_id already imported",
                              "ingest_batch_id": r["ingest_batch_id"],
                              "ingested_at": r["ingested_at"]} for r in existing],
                        )

                    ingest_id = await _record_import(conn, "applications", len(apps), True, None)
                    # Canonical write order (see the contract above).
                    ordered = sorted(apps, key=lambda a: a.application_id)
                    await conn.executemany(
                        """INSERT INTO applications
                               (application_id, customer_id, application_time,
                                ingested_at, ingest_batch_id)
                           VALUES ($1, $2, $3, now(), $4)""",
                        [(a.application_id, a.customer_id, a.application_time, ingest_id)
                         for a in ordered],
                    )
                    return ImportAccepted(
                        ingest_batch_id=ingest_id, imported=len(apps), kind="applications"
                    )
        except (asyncpg.UniqueViolationError, asyncpg.DeadlockDetectedError):
            # A concurrent import committed the same application_id between
            # our check and our write. Everything rolled back; re-check.
            continue

    # Every attempt collided with a concurrent import: decide on a fresh
    # snapshot without writing any data rows.
    async with pool.acquire() as conn:
        async with conn.transaction():
            existing = await conn.fetch(
                "SELECT application_id FROM applications WHERE application_id = ANY($1)",
                [a.application_id for a in apps],
            )
            ids = [r["application_id"] for r in existing]
            detail = (f"whole import rejected: application_id already exists {ids}"
                      if ids else
                      "whole import rejected: concurrent import collision, please retry")
            ingest_id = await _record_import(conn, "applications", len(apps), False, detail)
            return _reject_409(ingest_id, detail,
                               [{"application_id": i, "reason": "already imported"} for i in ids])


def _validate_credit_payload(events) -> tuple[dict, dict, list[dict]]:
    """Collapse identical duplicates; detect in-payload conflicts.

    Returns (versions, customer_of_event, conflicts): ``versions`` maps
    (event_id, known_time) to the value signature, ``customer_of_event`` maps
    event_id to its single customer_id, ``conflicts`` is non-empty when the
    payload itself is inconsistent.
    """
    versions: dict[tuple[str, datetime], dict] = {}
    conflicts: list[dict] = []
    customer_of_event: dict[str, str] = {}
    for e in events:
        key = (e.event_id, e.known_time)
        signature = {
            "customer_id": e.customer_id,
            "business_time": e.business_time,
            "income": e.income,
            "withdrawn": e.withdrawn,
        }
        if key in versions and versions[key] != signature:
            conflicts.append({
                "event_id": e.event_id,
                "known_time": e.known_time,
                "reason": "different values for same event_id+known_time in payload",
                "rejected": [versions[key], signature],
            })
        else:
            versions.setdefault(key, signature)
        prior_customer = customer_of_event.get(e.event_id)
        if prior_customer and prior_customer != e.customer_id:
            conflicts.append({
                "event_id": e.event_id,
                "reason": "event_id mapped to multiple customer_id values in payload",
                "rejected_customer_ids": sorted({prior_customer, e.customer_id}),
            })
        customer_of_event.setdefault(e.event_id, e.customer_id)
    return versions, customer_of_event, conflicts


async def _check_credit_events(conn, versions: dict, customer_of_event: dict
                               ) -> tuple[list[dict], dict]:
    """Diff payload versions against stored ones.

    Returns (conflicts, to_insert): ``conflicts`` names every stored version
    that disagrees with the payload (with the stored values and the batch
    that wrote them); ``to_insert`` holds the genuinely new versions.
    """
    conflicts: list[dict] = []
    to_insert: dict[tuple[str, datetime], dict] = {}
    if versions:
        db_rows = await conn.fetch(
            """SELECT event_id, known_time, customer_id, business_time,
                      income, withdrawn, ingested_at, ingest_batch_id
                 FROM credit_event_versions
                WHERE (event_id, known_time) IN (
                        SELECT v.e, v.k FROM unnest($1::text[], $2::timestamptz[])
                            AS v(e, k)
                    )""",
            [k[0] for k in versions], [k[1] for k in versions],
        )
        stored = {(r["event_id"], r["known_time"]): r for r in db_rows}
    else:
        stored = {}

    for key, sig in versions.items():
        row = stored.get(key)
        if row is None:
            to_insert[key] = sig  # genuinely new version
            continue
        if (sig["customer_id"] != row["customer_id"]
                or sig["business_time"] != row["business_time"]
                or sig["income"] != row["income"]
                or sig["withdrawn"] != row["withdrawn"]):
            conflicts.append({
                "event_id": key[0],
                "known_time": key[1],
                "reason": "different values already stored for event_id+known_time",
                "stored": {
                    "customer_id": row["customer_id"],
                    "business_time": row["business_time"],
                    "income": row["income"],
                    "withdrawn": row["withdrawn"],
                    "ingest_batch_id": row["ingest_batch_id"],
                    "ingested_at": row["ingested_at"],
                },
                "rejected": sig,
            })
        # Identical version already stored: idempotent retransmission, skip.

    # Customer consistency for events that already exist under other versions.
    if customer_of_event:
        known_events = await conn.fetch(
            """SELECT DISTINCT event_id, customer_id FROM credit_event_versions
                WHERE event_id = ANY($1)""",
            list(customer_of_event),
        )
        for row in known_events:
            if customer_of_event.get(row["event_id"]) != row["customer_id"]:
                conflicts.append({
                    "event_id": row["event_id"],
                    "reason": "event_id belongs to another customer_id",
                    "stored_customer_id": row["customer_id"],
                    "rejected_customer_id": customer_of_event[row["event_id"]],
                })
    return conflicts, to_insert


@app.post(
    "/api/v1/imports/credit-events",
    response_model=ImportAccepted,
    status_code=status.HTTP_201_CREATED,
    tags=["imports"],
    summary="导入征信变更日志（同事件同获知时间异值则整批拒绝）",
    responses={409: {"model": RejectedResponse, "description": "整批拒绝：负载内冲突或与已存版本冲突"}},
)
async def import_credit_events(
    payload: CreditEventsImport,
    demo_check_delay_seconds: float = Query(
        default=0.0, ge=0.0, le=30.0,
        description="演示/测试钩子：冲突检查通过后、写入前等待 N 秒，"
                    "放大并发交错窗口以复现竞态（生产保持 0）",
    ),
) -> ImportAccepted:
    events = payload.events
    versions, customer_of_event, payload_conflicts = _validate_credit_payload(events)

    pool = get_pool()
    for attempt in range(config.IMPORT_MAX_ATTEMPTS):
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    if payload_conflicts:
                        ingest_id = await _record_import(
                            conn, "credit_events", len(events), False,
                            "conflicting versions in payload",
                        )
                        return _reject_409(
                            ingest_id,
                            "whole import rejected: conflicting event versions in payload",
                            payload_conflicts,
                        )

                    conflicts, to_insert = await _check_credit_events(
                        conn, versions, customer_of_event
                    )
                    if conflicts:
                        ingest_id = await _record_import(
                            conn, "credit_events", len(events), False,
                            f"conflicts with stored versions ({len(conflicts)})",
                        )
                        return _reject_409(
                            ingest_id,
                            "whole import rejected: conflicts with stored versions",
                            conflicts,
                        )

                    # Demo hook: hold the (still empty-of-writes) transaction
                    # open after the check so a concurrent import can pass its
                    # own check too -- reproducing the interleaved-writes race.
                    if demo_check_delay_seconds and attempt == 0:
                        await asyncio.sleep(demo_check_delay_seconds)

                    ingest_id = await _record_import(conn, "credit_events", len(events), True, None)
                    # Canonical write order: overlapping batches lock the same
                    # keys in the same sequence (no deadlock), and the primary
                    # key lets exactly one of them commit each version.
                    for event_id, known_time in sorted(to_insert):
                        sig = to_insert[(event_id, known_time)]
                        await conn.execute(
                            """INSERT INTO credit_event_versions
                                   (event_id, customer_id, business_time, known_time,
                                    income, withdrawn, ingested_at, ingest_batch_id)
                               VALUES ($1,$2,$3,$4,$5,$6,now(),$7)""",
                            event_id, sig["customer_id"], sig["business_time"], known_time,
                            sig["income"], sig["withdrawn"], ingest_id,
                        )
                    return ImportAccepted(
                        ingest_batch_id=ingest_id, imported=len(to_insert), kind="credit_events"
                    )
        except (asyncpg.UniqueViolationError, asyncpg.DeadlockDetectedError):
            # A concurrent import committed a version we had just found
            # absent. The whole attempt rolled back -- none of this batch's
            # new rows and no ledger entry survived. Re-check against the
            # fresh committed state and let the checks decide (409 on a real
            # conflict, idempotent 201 on an identical retransmission).
            continue

    # All attempts collided with concurrent imports. Make the final decision
    # on a fresh snapshot without writing any data rows.
    async with pool.acquire() as conn:
        async with conn.transaction():
            conflicts, to_insert = await _check_credit_events(conn, versions, customer_of_event)
            if conflicts:
                ingest_id = await _record_import(
                    conn, "credit_events", len(events), False,
                    f"conflicts with stored versions ({len(conflicts)})",
                )
                return _reject_409(
                    ingest_id,
                    "whole import rejected: conflicts with stored versions",
                    conflicts,
                )
            if not to_insert:
                # Every version is already stored exactly as submitted: the
                # concurrent twin committed them, so this is an idempotent
                # retransmission.
                ingest_id = await _record_import(
                    conn, "credit_events", len(events), True,
                    "idempotent retransmission; all versions already stored",
                )
                return ImportAccepted(ingest_batch_id=ingest_id, imported=0, kind="credit_events")
            ingest_id = await _record_import(
                conn, "credit_events", len(events), False,
                "concurrent imports collided repeatedly; safe to retry",
            )
            return _reject_409(
                ingest_id,
                "whole import rejected: concurrent import collision, please retry",
                [],
            )


# --------------------------------------------------------------------------- #
# Batch generation
# --------------------------------------------------------------------------- #
@app.post(
    "/api/v1/batches",
    response_model=BatchResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["batches"],
    summary="生成回填批次（固化受理时刻已提交的输入）",
)
async def create_batch(payload: BatchCreateRequest) -> BatchResponse:
    ids = payload.application_ids
    if len(set(ids)) != len(ids):
        raise HTTPException(400, "duplicate application_ids in request")

    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                "SELECT application_id FROM applications WHERE application_id = ANY($1) ORDER BY application_id",
                ids,
            )
            found = {r["application_id"] for r in rows}
            missing = sorted(set(ids) - found)
            if missing:
                raise HTTPException(400, f"unknown application_ids: {missing}")

            batch_id = uuid.uuid4()
            batch_row = await conn.fetchrow(
                """INSERT INTO batches (batch_id, frozen_ingest_at, total_items)
                   VALUES ($1, now(), $2)
                   RETURNING batch_id, created_at, frozen_ingest_at, status,
                             total_items, processed_items, worker_id, error""",
                batch_id, len(ids),
            )
            await conn.executemany(
                """INSERT INTO batch_items (batch_id, application_id, item_seq)
                   VALUES ($1, $2, $3)""",
                [(batch_id, app_id, seq) for seq, app_id in enumerate(sorted(ids))],
            )
    return BatchResponse(**dict(batch_row.items()))


@app.get("/api/v1/batches", response_model=list[BatchResponse], tags=["batches"],
         summary="列出全部批次及状态")
async def list_batches() -> list[BatchResponse]:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT batch_id, created_at, frozen_ingest_at, status, total_items,
                      processed_items, worker_id, error
                 FROM batches ORDER BY created_at""",
        )
    return [BatchResponse(**dict(r.items())) for r in rows]


@app.get("/api/v1/batches/{batch_id}", response_model=BatchResponse, tags=["batches"],
         summary="查询单个批次状态")
async def get_batch(batch_id: uuid.UUID) -> BatchResponse:
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT batch_id, created_at, frozen_ingest_at, status, total_items,
                      processed_items, worker_id, error
                 FROM batches WHERE batch_id = $1""",
            batch_id,
        )
    if not row:
        raise HTTPException(404, "batch not found")
    return BatchResponse(**dict(row.items()))


@app.post("/api/v1/batches/{batch_id}/resume", response_model=BatchResponse, tags=["batches"],
          summary="将 FAILED 批次重新置为 PENDING（续算，不改变已固化输入）")
async def resume_batch(batch_id: uuid.UUID) -> BatchResponse:
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """UPDATE batches
                  SET status = 'PENDING', worker_id = NULL,
                      lease_expires_at = NULL, error = NULL
                WHERE batch_id = $1 AND status = 'FAILED'
            RETURNING batch_id, created_at, frozen_ingest_at, status, total_items,
                      processed_items, worker_id, error""",
            batch_id,
        )
        if not row:
            raise HTTPException(409, "only FAILED batches can be resumed")
        await conn.execute(
            """UPDATE batch_items
                  SET status = 'PENDING', worker_id = NULL, lease_expires_at = NULL
                WHERE batch_id = $1 AND status <> 'DONE'""",
            batch_id,
        )
    return BatchResponse(**dict(row.items()))


# --------------------------------------------------------------------------- #
# Results — keyset pagination, only after COMPLETED
# --------------------------------------------------------------------------- #
def _encode_cursor(item_seq: int) -> str:
    return base64.urlsafe_b64encode(str(item_seq).encode()).decode()


def _decode_cursor(cursor: str) -> int:
    try:
        return int(base64.urlsafe_b64decode(cursor.encode()).decode())
    except Exception as exc:
        raise HTTPException(400, "invalid cursor") from exc


@app.get("/api/v1/batches/{batch_id}/results", response_model=ResultsPage, tags=["results"],
         summary="分页查询结果（批次全部完成后才开放；成功批次内容不再变化）")
async def get_results(
    batch_id: uuid.UUID,
    limit: int = Query(default=config.DEFAULT_PAGE_SIZE, ge=1, le=config.MAX_PAGE_SIZE),
    cursor: str | None = Query(default=None, description="上一页返回的 next_cursor"),
) -> ResultsPage:
    after_seq = _decode_cursor(cursor) if cursor else -1
    pool = get_pool()
    async with pool.acquire() as conn:
        b = await conn.fetchrow("SELECT status, error FROM batches WHERE batch_id = $1", batch_id)
        if not b:
            raise HTTPException(404, "batch not found")
        if b["status"] != "COMPLETED":
            raise HTTPException(
                409,
                f"results not open: batch is {b['status']}"
                + (f" ({b['error']})" if b["error"] else ""),
            )
        rows = await conn.fetch(
            """SELECT application_id, customer_id, application_time, income,
                      source_event_id, source_version_known_time,
                      source_business_time, source_ingest_batch_id, item_seq
                 FROM batch_results
                WHERE batch_id = $1 AND item_seq > $2
                ORDER BY item_seq
                LIMIT $3""",
            batch_id, after_seq, limit + 1,
        )
    next_cursor = None
    if len(rows) > limit:
        next_cursor = _encode_cursor(rows[limit - 1]["item_seq"])
        rows = rows[:limit]
    items = [
        ResultRow(
            application_id=r["application_id"],
            customer_id=r["customer_id"],
            application_time=r["application_time"],
            income=r["income"],
            source_event_id=r["source_event_id"],
            source_version_known_time=r["source_version_known_time"],
            source_business_time=r["source_business_time"],
            source_ingest_batch_id=r["source_ingest_batch_id"],
            basis="event" if r["source_event_id"] else "no_known_event",
        )
        for r in rows
    ]
    return ResultsPage(batch_id=batch_id, status="COMPLETED", limit=limit,
                       next_cursor=next_cursor, items=items)


# --------------------------------------------------------------------------- #
# Debug / inspection helpers (used by the demo script and /docs)
# --------------------------------------------------------------------------- #
@app.get("/api/v1/imports", tags=["inspect"],
         summary="查看导入批次登记（受理/拒绝及原因，与实际写入一致）")
async def list_imports() -> list[dict]:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT ingest_batch_id, import_kind, row_count, status, reason, received_at
                 FROM import_batches ORDER BY received_at""",
        )
    return [dict(r.items()) for r in rows]


@app.get("/api/v1/applications", tags=["inspect"], summary="查看已导入申请")
async def list_applications() -> list[dict]:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT application_id, customer_id, application_time,
                      ingested_at, ingest_batch_id
                 FROM applications ORDER BY application_id""",
        )
    return [dict(r.items()) for r in rows]


@app.get("/api/v1/events", tags=["inspect"], summary="查看征信事件全部版本")
async def list_events() -> list[dict]:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT event_id, customer_id, business_time, known_time, income,
                      withdrawn, ingested_at, ingest_batch_id
                 FROM credit_event_versions
                ORDER BY event_id, known_time""",
        )
    return [dict(r.items()) for r in rows]
