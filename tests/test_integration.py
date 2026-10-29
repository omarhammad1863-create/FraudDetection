"""Real PostgreSQL, Redis and gRPC tests; no global database/Redis resets."""

import asyncio
import os
import socket
import sys
import unittest
from contextlib import AsyncExitStack
from dataclasses import replace
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import asyncpg
import grpc
import httpx
from redis.asyncio import Redis
from redis.backoff import NoBackoff
from redis.retry import Retry

import fraud_pb2
import fraud_pb2_grpc
from fraud_service import FraudDetector
from payment_service import create_app
from settings import Settings
from storage import fingerprint, reserve_request
from velocity import record_attempt, velocity_keys


class ControlledFraud(FraudDetector):
    """Fault injection at the real gRPC server boundary."""

    def __init__(self):
        self.mode = "normal"
        self.calls = []
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.on_call = None

    async def Evaluate(self, request, context):
        self.calls.append((request.amount_cents, request.tx_count_5m))
        self.entered.set()
        if self.on_call:
            await self.on_call()
        if self.mode == "hold":
            await self.release.wait()
        if self.mode == "timeout":
            await asyncio.sleep(2)
        if self.mode == "unavailable":
            await context.abort(grpc.StatusCode.UNAVAILABLE, "Injected outage")
        if self.mode == "invalid":
            return fraud_pb2.FraudResponse(decision="UNKNOWN")
        return await super().Evaluate(request, context)


class IntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stack = AsyncExitStack()
        self.addAsyncCleanup(self.stack.aclose)
        self.base = Settings.from_env()
        self.admin = await asyncpg.connect(self.base.database_url, timeout=3)
        self.stack.push_async_callback(self.admin.close)
        self.schema = "sentinel_test_" + uuid4().hex
        await self.admin.execute(f'CREATE SCHEMA "{self.schema}"')
        self.stack.push_async_callback(self.drop_schema)
        parts = urlsplit(self.base.database_url)
        query = dict(parse_qsl(parts.query))
        query["search_path"] = self.schema
        query["application_name"] = self.schema
        database_url = urlunsplit(parts._replace(query=urlencode(query)))

        self.fraud = ControlledFraud()
        self.server = grpc.aio.server()
        fraud_pb2_grpc.add_FraudDetectorServicer_to_server(self.fraud, self.server)
        port = self.server.add_insecure_port("127.0.0.1:0")
        await self.server.start()
        self.stack.push_async_callback(self.server.stop, 0)
        self.settings = replace(self.base, database_url=database_url, fraud_target=f"127.0.0.1:{port}")
        self.customers = set()
        self.redis = Redis.from_url(self.base.redis_url, decode_responses=True)
        await self.redis.ping()
        self.stack.push_async_callback(self.redis.aclose)
        self.stack.push_async_callback(self.clean_redis)
        self.app, self.client = await self.new_app()
        self.customer = self.new_customer()

    async def drop_schema(self):
        # The name is generated above, never taken from user input.
        await self.admin.execute(f'DROP SCHEMA "{self.schema}" CASCADE')

    async def clean_redis(self):
        for customer in self.customers:
            prefix = velocity_keys(customer, "unused")[0].removesuffix(":attempts")
            keys = [key async for key in self.redis.scan_iter(match=prefix + ":*")]
            if keys:
                await self.redis.delete(*keys)

    def new_customer(self):
        customer = "test_" + uuid4().hex
        self.customers.add(customer)
        return customer

    async def new_app(self, settings=None):
        app = create_app(settings or self.settings)
        await self.stack.enter_async_context(app.router.lifespan_context(app))
        client = await self.stack.enter_async_context(httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ))
        return app, client

    async def post(self, key=None, amount=150, customer=None, client=None):
        return await (client or self.client).post(
            "/pay", json={"customer_id": customer or self.customer, "amount": amount},
            headers={"Idempotency-Key": key or uuid4().hex},
        )

    async def row_count(self):
        return await self.app.state.database.fetchval("SELECT count(*) FROM payment_evaluations")

    async def test_saved_result_can_be_read(self):
        response = await self.post(amount="150.75")
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual((body["amount_cents"], body["decision"], body["tx_count_5m"]), (15075, "ALLOW", 1))
        saved = await self.client.get("/payments/" + body["id"])
        self.assertEqual(saved.json(), body)
        self.assertEqual(await self.row_count(), 1)

    async def test_amount_boundaries(self):
        allowed = await self.post(amount=10000)
        blocked = await self.post(amount="10000.01")
        self.assertEqual(allowed.json()["decision"], "ALLOW")
        self.assertEqual(blocked.json()["decision"], "BLOCK")
        self.assertEqual(await self.row_count(), 2)

    async def test_fourth_attempt_blocks_and_customers_are_isolated(self):
        bodies = [(await self.post()).json() for _ in range(4)]
        self.assertEqual([b["tx_count_5m"] for b in bodies], [1, 2, 3, 4])
        self.assertEqual([b["decision"] for b in bodies], ["ALLOW", "ALLOW", "ALLOW", "BLOCK"])
        other = await self.post(customer=self.new_customer())
        self.assertEqual(other.json()["tx_count_5m"], 1)

    async def test_replay_does_not_recount_or_recheck(self):
        key = uuid4().hex
        first = await self.post(key, amount=150)
        await self.post()
        replay = await self.post(key, amount="150.00")
        self.assertEqual(replay.status_code, 200)
        self.assertEqual(replay.json(), first.json())
        self.assertEqual(replay.headers["Idempotent-Replay"], "true")
        self.assertEqual(len(self.fraud.calls), 2)
        self.assertEqual(await self.row_count(), 2)
        self.assertEqual((await self.post()).json()["tx_count_5m"], 3)

    async def test_changed_payload_rejected(self):
        key = uuid4().hex
        await self.post(key)
        self.assertEqual((await self.post(key, amount=151)).status_code, 422)
        self.assertEqual((await self.post(key, customer=self.new_customer())).status_code, 422)
        self.assertEqual(await self.row_count(), 1)
        self.assertEqual(len(self.fraud.calls), 1)

    async def test_concurrent_duplicate_across_application_instances(self):
        second_app, second_client = await self.new_app()
        # Connection setup is not the behavior under test and must not consume
        # the held RPC's one-second deadline on a cold Windows/Docker machine.
        connections = await asyncio.gather(*[
            second_app.state.database.acquire() for _ in range(8)
        ])
        for connection in connections:
            await second_app.state.database.release(connection)
        self.fraud.mode = "hold"
        key = uuid4().hex
        first_task = asyncio.create_task(self.post(key))
        try:
            await asyncio.wait_for(self.fraud.entered.wait(), 1)
            duplicates = await asyncio.gather(*[
                self.post(key, client=second_client) for _ in range(8)
            ])
            self.assertTrue(all(r.status_code == 409 for r in duplicates), [r.text for r in duplicates])
            self.assertEqual((await self.post(key, amount=151, client=second_client)).status_code, 422)
        finally:
            self.fraud.release.set()
            first = await first_task
        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(len(self.fraud.calls), 1)
        self.assertEqual(await self.row_count(), 1)
        self.assertEqual((await self.post(key, client=second_client)).json(), first.json())

    async def test_simultaneous_first_reservations_create_one_evaluation(self):
        _, second_client = await self.new_app()
        key = uuid4().hex
        responses = await asyncio.gather(*[
            self.post(key, client=self.client if index % 2 else second_client)
            for index in range(10)
        ])
        self.assertTrue(all(r.status_code in {200, 409} for r in responses), [r.text for r in responses])
        successes = [r.json() for r in responses if r.status_code == 200]
        self.assertTrue(successes)
        self.assertTrue(all(body == successes[0] for body in successes))
        self.assertEqual(await self.row_count(), 1)
        self.assertEqual(len(self.fraud.calls), 1)
        attempts_key, _ = velocity_keys(self.customer, "unused")
        self.assertEqual(await self.redis.zcard(attempts_key), 1)

    async def test_concurrent_distinct_attempts_have_atomic_counts(self):
        responses = await asyncio.gather(*[self.post() for _ in range(8)])
        self.assertTrue(all(r.status_code == 200 for r in responses), [r.text for r in responses])
        self.assertEqual(sorted(r.json()["tx_count_5m"] for r in responses), list(range(1, 9)))
        self.assertEqual(sum(r.json()["decision"] == "BLOCK" for r in responses), 5)

    async def test_rolling_window_removes_old_attempts(self):
        attempt = uuid4()
        key, _ = velocity_keys(self.customer, attempt)
        seconds, micros = await self.redis.time()
        now_ms = seconds * 1000 + micros // 1000
        await self.redis.zadd(key, {"expired": now_ms - 301000, "recent": now_ms - 1000})
        count = await record_attempt(self.redis, self.customer, attempt)
        self.assertEqual(count, 2)
        self.assertIsNone(await self.redis.zscore(key, "expired"))
        self.assertTrue(0 < await self.redis.ttl(key) <= 300)
        # Retrying this same attempt keeps its original snapshot.
        await record_attempt(self.redis, self.customer, uuid4())
        self.assertEqual(await record_attempt(self.redis, self.customer, attempt), 2)

    async def test_grpc_failure_then_retry_does_not_double_count(self):
        key = uuid4().hex
        self.fraud.mode = "unavailable"
        failed = await self.post(key)
        self.assertEqual(failed.status_code, 503)
        self.assertEqual(await self.row_count(), 0)
        self.assertEqual((await self.post(key, amount=151)).status_code, 422)
        self.fraud.mode = "normal"
        succeeded = await self.post(key)
        self.assertEqual(succeeded.json()["tx_count_5m"], 1)
        self.assertEqual(await self.row_count(), 1)

    async def test_deadline_does_not_save_an_approval(self):
        self.fraud.mode = "timeout"
        key = uuid4().hex
        self.assertEqual((await self.post(key)).status_code, 504)
        self.assertEqual(await self.row_count(), 0)
        self.fraud.mode = "normal"
        self.assertEqual((await self.post(key)).json()["tx_count_5m"], 1)

    async def test_invalid_fraud_response_is_not_saved(self):
        self.fraud.mode = "invalid"
        self.assertEqual((await self.post()).status_code, 502)
        self.assertEqual(await self.row_count(), 0)

    async def test_redis_unavailable_fails_closed_but_saved_replay_works(self):
        key = uuid4().hex
        original = await self.post(key)
        broken = Redis.from_url(
            "redis://127.0.0.1:1", socket_connect_timeout=.1, socket_timeout=.1,
            retry=Retry(NoBackoff(), 0),
        )
        self.stack.push_async_callback(broken.aclose)
        self.app.state.redis = broken
        fresh_key = uuid4().hex
        self.assertEqual((await self.post(fresh_key)).status_code, 503)
        self.assertEqual((await self.post(key)).json(), original.json())
        self.assertEqual(len(self.fraud.calls), 1)
        self.assertEqual(await self.row_count(), 1)
        self.app.state.redis = self.redis
        self.assertEqual((await self.post(fresh_key)).json()["tx_count_5m"], 2)

    async def test_restart_replays_without_fraud_or_redis(self):
        key = uuid4().hex
        original = await self.post(key)
        restarted_settings = replace(self.settings, redis_url="redis://127.0.0.1:1", fraud_target="127.0.0.1:1")
        _, restarted = await self.new_app(restarted_settings)
        replay = await self.post(key, client=restarted)
        self.assertEqual(replay.json(), original.json())
        self.assertEqual(replay.headers["Idempotent-Replay"], "true")
        self.assertEqual(await self.row_count(), 1)

    async def test_database_disconnect_rolls_back_and_retry_recovers(self):
        key = uuid4().hex

        async def disconnect_processing_connection():
            pids = await self.admin.fetch(
                "SELECT pid FROM pg_stat_activity WHERE application_name = $1 "
                "AND state = 'idle in transaction' "
                "AND query LIKE 'SELECT * FROM idempotency_requests%FOR UPDATE NOWAIT'",
                self.schema,
            )
            self.assertEqual(len(pids), 1)
            await self.admin.execute("SELECT pg_terminate_backend($1)", pids[0]["pid"])

        self.fraud.on_call = disconnect_processing_connection
        self.assertEqual((await self.post(key)).status_code, 503)
        self.assertEqual(await self.row_count(), 0)
        self.fraud.on_call = None
        response = await self.post(key)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["tx_count_5m"], 1)
        self.assertEqual(await self.row_count(), 1)

    async def test_atomic_result_and_replay_rollback_on_save_error(self):
        # Force an actual PostgreSQL error AFTER the evaluation INSERT, while
        # updating the replay record. Both writes must roll back together.
        await self.app.state.database.execute("""
            CREATE FUNCTION reject_response() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'Injected write failure'; END $$;
            CREATE TRIGGER reject_response BEFORE UPDATE ON idempotency_requests
            FOR EACH ROW EXECUTE FUNCTION reject_response();
        """)
        key = uuid4().hex
        self.assertEqual((await self.post(key)).status_code, 503)
        self.assertEqual(await self.row_count(), 0)
        self.assertIsNone(await self.app.state.database.fetchval(
            "SELECT response_body FROM idempotency_requests WHERE idempotency_key = $1", key
        ))
        await self.app.state.database.execute("DROP TRIGGER reject_response ON idempotency_requests")
        self.assertEqual((await self.post(key)).json()["tx_count_5m"], 1)

    async def test_pending_reservation_recovers_after_owner_disconnects(self):
        key = uuid4().hex
        owner = await asyncpg.connect(self.settings.database_url)
        try:
            reservation = await reserve_request(owner, key, fingerprint(self.customer, 15000))
            await owner.execute("BEGIN")
            await owner.fetchrow(
                "SELECT * FROM idempotency_requests WHERE idempotency_key = $1 FOR UPDATE", key
            )
            await record_attempt(self.redis, self.customer, reservation["attempt_id"])
        finally:
            await owner.close()  # Simulate a process exiting without committing.
        response = await self.post(key)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["id"], str(reservation["attempt_id"]))
        self.assertEqual(response.json()["tx_count_5m"], 1)

    async def test_invalid_requests_never_reach_redis_or_grpc(self):
        for amount in [0, -1, "1.001", "10000000000", "NaN", "Infinity"]:
            with self.subTest(amount=amount):
                self.assertEqual((await self.post(amount=amount)).status_code, 422)
        for payload in [{"amount": 150}, {"customer_id": "", "amount": 150},
                        {"customer_id": "a", "amount": 150, "currency": "USD"}]:
            response = await self.client.post("/pay", json=payload, headers={"Idempotency-Key": uuid4().hex})
            self.assertEqual(response.status_code, 422)
        response = await self.client.post("/pay", json={"customer_id": self.customer, "amount": 150})
        self.assertEqual(response.status_code, 422)
        self.assertEqual(len(self.fraud.calls), 0)
        self.assertEqual(await self.row_count(), 0)
        self.assertEqual(await self.app.state.database.fetchval("SELECT count(*) FROM idempotency_requests"), 0)

    async def test_missing_evaluation_and_bad_uuid(self):
        self.assertEqual((await self.client.get(f"/payments/{uuid4()}")).status_code, 404)
        self.assertEqual((await self.client.get("/payments/not-a-uuid")).status_code, 422)

    async def test_grpc_validates_direct_clients(self):
        async with grpc.aio.insecure_channel(self.settings.fraud_target) as channel:
            stub = fraud_pb2_grpc.FraudDetectorStub(channel)
            for amount, count in [(0, 1), (15000, 0), (15000, -1)]:
                with self.assertRaises(grpc.aio.AioRpcError) as error:
                    await stub.Evaluate(fraud_pb2.FraudRequest(amount_cents=amount, tx_count_5m=count))
                self.assertEqual(error.exception.code(), grpc.StatusCode.INVALID_ARGUMENT)

    async def test_real_http_process_restart_and_persistent_replay(self):
        # Exercise Uvicorn and the actual fraud_service.py in separate processes.
        def free_port():
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                return listener.getsockname()[1]

        grpc_port, http_port = free_port(), free_port()
        env = dict(os.environ, DATABASE_URL=self.settings.database_url,
                   REDIS_URL=self.settings.redis_url, FRAUD_TARGET=f"127.0.0.1:{grpc_port}",
                   FRAUD_BIND=f"127.0.0.1:{grpc_port}")
        root = str(Path(__file__).resolve().parents[1])

        async def start(*args):
            process = await asyncio.create_subprocess_exec(
                sys.executable, *args, env=env, cwd=root,
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            )
            self.stack.push_async_callback(stop, process)
            return process

        async def stop(process):
            if process.returncode is None:
                process.terminate()
            await process.wait()

        fraud_process = await start("fraud_service.py")
        async with grpc.aio.insecure_channel(env["FRAUD_TARGET"]) as channel:
            await asyncio.wait_for(channel.channel_ready(), 10)
        api_args = ("-m", "uvicorn", "payment_service:app", "--host", "127.0.0.1", "--port", str(http_port))

        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{http_port}", timeout=5) as client:
            async def wait_ready():
                for _ in range(100):
                    try:
                        if (await client.get("/openapi.json")).status_code == 200:
                            return
                    except httpx.TransportError:
                        pass
                    await asyncio.sleep(.1)
                self.fail("Uvicorn did not become ready")

            api_process = await start(*api_args)
            await wait_ready()
            key = uuid4().hex
            original = await self.post(key, client=client)
            self.assertEqual(original.status_code, 200, original.text)
            await stop(api_process)
            await stop(fraud_process)
            await start(*api_args)
            await wait_ready()
            replay = await self.post(key, client=client)
            self.assertEqual(replay.json(), original.json())
            self.assertEqual(replay.headers["Idempotent-Replay"], "true")


if __name__ == "__main__":
    unittest.main()
