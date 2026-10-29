"""Receive HTTP requests, count attempts, evaluate risk, and save replayable results."""

import asyncio
import json
import logging
from contextlib import AsyncExitStack, asynccontextmanager
from decimal import Decimal
from typing import Annotated
from uuid import UUID

import asyncpg
import grpc
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from redis.asyncio import Redis
from redis.backoff import NoBackoff
from redis.exceptions import RedisError
from redis.retry import Retry

import fraud_pb2
import fraud_pb2_grpc
from settings import Settings
from storage import evaluation_body, fingerprint, migrate, reserve_request, save_evaluation
from velocity import record_attempt


logger = logging.getLogger(__name__)
DB_ERRORS = (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, asyncio.TimeoutError)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = app.state.settings
    async with AsyncExitStack() as stack:
        pool = await stack.enter_async_context(asyncpg.create_pool(
            settings.database_url, min_size=1, max_size=10,
            timeout=3, command_timeout=3,
        ))
        await migrate(pool)
        redis = Redis.from_url(
            settings.redis_url, socket_connect_timeout=1, socket_timeout=1,
            retry=Retry(NoBackoff(), 0), decode_responses=True,
        )
        stack.push_async_callback(redis.aclose)
        channel = await stack.enter_async_context(
            grpc.aio.insecure_channel(settings.fraud_target)
        )
        app.state.database = pool
        app.state.redis = redis
        app.state.fraud = fraud_pb2_grpc.FraudDetectorStub(channel)
        yield


class PaymentRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    customer_id: Annotated[str, Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_-]+$")]
    amount: Annotated[Decimal, Field(gt=0, max_digits=12, decimal_places=2)]


IdempotencyKey = Annotated[
    str, Header(alias="Idempotency-Key", min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._:-]+$")
]


async def evaluate_and_save(connection, request: Request, payment: PaymentRequest, key: str):
    amount_cents = int(payment.amount * 100)
    request_hash = fingerprint(payment.customer_id, amount_cents)
    reservation = await reserve_request(connection, key, request_hash)
    if reservation["request_hash"] != request_hash:
        raise HTTPException(422, "Idempotency-Key already belongs to a different request")

    # The row lock works across processes, and PostgreSQL releases it on rollback
    # or connection loss. Downstream operations have short, explicit timeouts.
    async with connection.transaction():
        reservation = await connection.fetchrow(
            "SELECT * FROM idempotency_requests WHERE idempotency_key = $1 FOR UPDATE NOWAIT",
            key,
        )
        if reservation["response_body"] is not None:
            return JSONResponse(
                json.loads(reservation["response_body"]),
                headers={"Idempotent-Replay": "true"},
            )

        attempt_id = reservation["attempt_id"]
        count = await record_attempt(request.app.state.redis, payment.customer_id, attempt_id)
        result = await request.app.state.fraud.Evaluate(
            fraud_pb2.FraudRequest(amount_cents=amount_cents, tx_count_5m=count),
            timeout=1.0,
        )
        if result.decision not in {"ALLOW", "BLOCK"}:
            raise HTTPException(502, "Fraud service returned an invalid decision")
        body = await save_evaluation(
            connection, key, attempt_id, payment.customer_id, amount_cents, count, result.decision
        )
    # Only send success AFTER the transaction has committed.
    return JSONResponse(body)


async def pay(payment: PaymentRequest, request: Request, idempotency_key: IdempotencyKey):
    """Evaluate and save an EGP attempt. This does not charge or settle money."""
    try:
        async with request.app.state.database.acquire(timeout=3) as connection:
            return await evaluate_and_save(connection, request, payment, idempotency_key)
    except asyncpg.LockNotAvailableError as exc:
        raise HTTPException(409, "This request is already processing; retry the same key") from exc
    except RedisError as exc:
        raise HTTPException(503, "Velocity check unavailable; retry the same key") from exc
    except grpc.aio.AioRpcError as exc:
        if exc.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
            raise HTTPException(504, "Fraud check timed out; retry the same key") from exc
        if exc.code() == grpc.StatusCode.UNAVAILABLE:
            raise HTTPException(503, "Fraud service unavailable; retry the same key") from exc
        raise HTTPException(502, "Fraud check failed; retry the same key") from exc
    except DB_ERRORS as exc:
        logger.warning("PostgreSQL request failed (%s)", type(exc).__name__)
        raise HTTPException(503, "Payment storage unavailable; retry the same key") from exc


async def get_payment(payment_id: UUID, request: Request):
    """Read a saved evaluation. This local learning API has no authentication yet."""
    try:
        async with request.app.state.database.acquire(timeout=3) as connection:
            row = await connection.fetchrow(
                "SELECT * FROM payment_evaluations WHERE id = $1", payment_id
            )
    except DB_ERRORS as exc:
        raise HTTPException(503, "Payment storage unavailable") from exc
    if row is None:
        raise HTTPException(404, "Evaluation not found")
    return evaluation_body(row)


def create_app(settings: Settings | None = None) -> FastAPI:
    app = FastAPI(title="SentinelPay — Stored Evaluations", lifespan=lifespan)
    app.state.settings = settings or Settings.from_env()
    app.post("/pay")(pay)
    app.get("/payments/{payment_id}")(get_payment)
    return app


app = create_app()
