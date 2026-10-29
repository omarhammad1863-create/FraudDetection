"""PostgreSQL owns durable requests, evaluation records, and replay responses."""

import hashlib
import json
from pathlib import Path
from uuid import uuid4

import asyncpg


async def migrate(pool: asyncpg.Pool):
    """Apply numbered SQL files once; concurrent application starts are safe."""
    async with pool.acquire() as connection:
        async with connection.transaction():
            await connection.execute("SELECT pg_advisory_xact_lock(738294102)")
            await connection.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations "
                "(version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
            )
            for path in sorted((Path(__file__).parent / "migrations").glob("*.sql")):
                applied = await connection.fetchval(
                    "SELECT 1 FROM schema_migrations WHERE version = $1", path.name
                )
                if not applied:
                    await connection.execute(path.read_text(encoding="utf-8"))
                    await connection.execute(
                        "INSERT INTO schema_migrations(version) VALUES ($1)", path.name
                    )


def fingerprint(customer_id: str, amount_cents: int) -> str:
    # 150, 150.0 and 150.00 identify the same monetary payload.
    canonical = json.dumps([customer_id, amount_cents, "EGP"], separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


async def reserve_request(connection, key: str, request_hash: str):
    # This INSERT commits BEFORE the processing transaction. A retry after a
    # failure therefore reuses the same attempt_id when talking to Redis.
    await connection.execute(
        "INSERT INTO idempotency_requests(idempotency_key, request_hash, attempt_id) "
        "VALUES ($1, $2, $3) ON CONFLICT (idempotency_key) DO NOTHING",
        key, request_hash, uuid4(),
    )
    return await connection.fetchrow(
        "SELECT * FROM idempotency_requests WHERE idempotency_key = $1", key
    )


def evaluation_body(row) -> dict:
    return {
        "id": str(row["id"]),
        "customer_id": row["customer_id"],
        "amount_cents": row["amount_cents"],
        "currency": "EGP",
        "decision": row["decision"],
        "tx_count_5m": row["tx_count_5m"],
        "created_at": row["created_at"].isoformat(),
    }


async def save_evaluation(connection, key, attempt_id, customer_id, amount_cents, count, decision):
    # The caller's transaction commits BOTH the evaluation and its replay body.
    row = await connection.fetchrow(
        "INSERT INTO payment_evaluations(id, customer_id, amount_cents, tx_count_5m, decision) "
        "VALUES ($1, $2, $3, $4, $5) RETURNING *",
        attempt_id, customer_id, amount_cents, count, decision,
    )
    body = evaluation_body(row)
    await connection.execute(
        "UPDATE idempotency_requests SET response_body = $2::jsonb WHERE idempotency_key = $1",
        key, json.dumps(body),
    )
    return body
