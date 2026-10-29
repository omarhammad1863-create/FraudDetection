# SentinelPay (v2.0): B2B Fraud & Payment Orchestration Platform
**Enterprise Architecture Specification & Implementation Blueprint**

---

## 1. Executive Summary & Product Vision

### 1.1 Business Problem (The Engineering Story)
Modern B2B payment orchestration platforms sit between merchants (e.g., e-commerce stores, ride-hailing apps) and upstream banking rails. They must solve six production-critical distributed systems challenges:
1. **Strict Latency Budget vs. Fraud Accuracy:** Evaluating risk synchronously during checkout must fit within an end-to-end $50\text{ ms}$ p99 latency budget. Calling external LLMs or running heavy SQL aggregations (`SELECT COUNT(*)`) per request causes checkout timeouts.
2. **Multi-Tenant Isolation (Noisy Neighbor & Data Leakage):** Customer activity (`user_123`) under `merchant_a` must never collide with `user_123` under `merchant_b` in Redis caches, PostgreSQL queries, or live WebSocket alert streams.
3. **Crash-Safe Idempotency (Stripe-Style):** Relying solely on Redis for idempotency fails if a service crashes after database commit but before cache update, or if Redis evicts keys. Furthermore, reusing an `Idempotency-Key` with a mutated payload (e.g., changing the amount) must be rejected (`HTTP 422`).
4. **Risk Decision vs. Financial State Separation:** A risk engine's opinion (`ALLOW`, `REVIEW`, `BLOCK`) is distinct from the bank's financial settlement state (`CREATED`, `AUTHORIZED`, `CAPTURED`, `VOIDED`, `REFUNDED`, `FAILED`).
5. **Upstream Provider Uncertainty & The Dual-Write Problem:** Bank providers experience network timeouts where a charge may or may not have succeeded, requiring deterministic reconciliation. Meanwhile, saving state changes to PostgreSQL and emitting events to Apache Kafka requires the **Transactional Outbox Pattern** plus **Idempotent Consumers** to guarantee at-least-once delivery without duplicate side effects.
6. **Velocity Attacks, Threshold Gaming & Pre-Settlement Clawback:** Attackers execute rapid transactions or stay just below static rule thresholds. When caught on the 4th attempt, operators must be able to freeze the customer and execute a **Retroactive Batch Reversal (`VOID` / `REFUND`)** on earlier transactions from the same attack window, backed by a full **Audit Log**.

### 1.2 The Product Deliverable
**SentinelPay** is a multi-tenant **B2B Fraud & Payment Orchestration Platform** backed by a **Simulated Upstream Bank Provider** (supporting configurable success, insufficient funds, latency injection, and network timeouts).
The system comprises a **3-microservice backend ecosystem** + a **Unified Split-Screen Next.js Dashboard**:
* **Left Panel (Merchant Checkout & Chaos/Attack Simulator):** Switch between merchants (`merchant_talabat`, `merchant_noon`), simulate normal checkouts, trigger concurrent duplicate requests, inject upstream bank timeouts, or launch multi-step velocity/threshold-gaming attacks.
* **Right Panel (Live Risk Operations Center):** Tenant-isolated, WebSocket-powered operations console with automatic state recovery on reconnect, displaying live risk alerts, correlated 15-minute attack chains, role-based operator controls (`viewer`, `analyst`, `admin` via lightweight headers), and one-click `Capture`, `Void`, and `Freeze & Retroactive Clawback` workflows.

---

## 2. System Architecture, Tech Stack & Latency Budget

### 2.1 Technology Stack (Pinned Versions)
* **Languages & Frameworks:** Python `3.11-slim`, FastAPI `0.115.0`, Pydantic `2.9.2`, `grpcio==1.66.2` & `grpcio-tools==1.66.2`.
* **Databases & Migrations:** PostgreSQL `16.4-alpine` (`asyncpg==0.29.0`, SQLAlchemy `2.0.35`, **Alembic `1.13.3`** for deterministic schema migrations). Single PostgreSQL instance partitioned logically into two schemas: `payment_core` (owned by `payment-service`) and `risk_ops_read` (read-model owned by `risk-ops-service`).
* **Caching & Atomic Counters:** Redis `7.4-alpine` (`redis==5.1.1`) using **Server-Side Lua Scripts** for sub-millisecond sliding-window velocity aggregation.
* **Message Broker:** Apache Kafka `3.8.0` (KRaft mode — no ZooKeeper) via `aiokafka==0.11.0`.
* **Fraud Engine:** Versioned Deterministic Rule Engine (`rules_v1`) + Optional Plug-in Anomaly Detection (`scikit-learn==1.5.2` `IsolationForest` via `joblib==1.4.2`).
* **Frontend:** Next.js `14.2`, React `18`, TypeScript `5`, Tailwind CSS `3.4`.

### 2.2 End-to-End Hot-Path Latency Budget ($50\text{ ms}$ Target p99)

| Step | Component | Target p95 | Hard Timeout / SLA | Fallback / Failure Strategy |
| :--- | :--- | :--- | :--- | :--- |
| **1. Edge Auth & Idempotency Check** | `payment-service` (Redis + PG) | $3\text{ ms}$ | $8\text{ ms}$ | Fail closed if PostgreSQL is down; bypass Redis if cache node is unreachable. |
| **2. Atomic Velocity Extraction** | `payment-service` -> Redis Lua | $1\text{ ms}$ | $4\text{ ms}$ | Default velocity features to `1` (degraded mode flag) if Redis times out. |
| **3. Fraud Evaluation RPC** | `payment-service` -> `fraud-engine` (gRPC) | $4\text{ ms}$ | **$15\text{ ms}$** | **Circuit Breaker Fallback:** Run local emergency static rule (`amount_cents > 5_000_000 -> REVIEW`, else `ALLOW`). |
| **4. Upstream Bank Authorization** | `payment-service` -> `PaymentProvider` | $10\text{ ms}$ | $20\text{ ms}$ | If timeout occurs, mark status `AUTHORIZED_PENDING_RECONCILIATION` and enqueue background reconciliation check. |
| **5. ACID State + Outbox Commit** | `payment-service` -> PostgreSQL | $5\text{ ms}$ | $10\text{ ms}$ | Rollback transaction on DB error; safe client retry via same `Idempotency-Key`. |
| **Total Synchronous Checkout** | **End-to-End Hot Path** | **$\approx 23\text{ ms}$** | **$50\text{ ms}$** | Measured and verified via automated load tests (`k6` / `Locust`). |

### 2.3 Microservices Topology & Data Ownership

```text
[Merchant Checkout / Simulator]
       │
       │ 1. POST /api/v1/payments
       │    Headers: X-API-Key, Idempotency-Key
       ▼
┌──────────────────────────────┐   2. Atomic Lua Velocity (<1ms)   ┌─────────────────────────┐
│                              │ ◄───────────────────────────────► │     Redis 7.4 Alpine    │
│       payment-service        │                                   │ (Tenant-Scoped ZSETs &  │
│        (FastAPI :8000)       │   3. gRPC Protobuf (15ms SLA)     │  Frozen Customer Sets)  │
│  Sole Owner of Payment State │ ────────────────────────────────► └─────────────────────────┘
│                              │ ◄──────────────────────────────── ┌─────────────────────────┐
└──────────────────────────────┘   4. RiskDecision (ALLOW/REVIEW/  │  fraud-engine-service   │
       │                              BLOCK) + Rule/Model Version  │   (Pure gRPC :50051)    │
       │ 5. Call MockPaymentProvider                               │  Versioned Rules + ML   │
       │    (Authorize / Capture)                                  └─────────────────────────┘
       │ 6. Single ACID Commit (Schema: payment_core):
       │    payments + idempotency_records + outbox_events + audit_logs
       ▼
┌──────────────────────────────┐
│    PostgreSQL 16.4 Alpine    │ ◄─── 11. Synchronous Internal REST Commands
│  - Schema: payment_core      │          (/internal/v1/payments/{id}/capture | void | clawback)
│  - Schema: risk_ops_read     │                                                  │
└──────────────────────────────┘                                                  │
       │                                                                          │
       │ 7. Poll PENDING outbox (SKIP LOCKED)                                     │
       ▼                                                                          │
┌──────────────────────────────┐   8. Publish Event (with event_id) ┌─────────────┴───────────┐
│     Outbox Relay Worker      │ ─────────────────────────────────► │   Apache Kafka 3.8.0    │
└──────────────────────────────┘                                    │ Topic: payment.events   │
                                                                    └─────────────┬───────────┘
                                                                                  │
                                                                    9. Idempotent Consume
                                                                       (Check processed_events)
                                                                                  ▼
                                                                    ┌─────────────────────────┐
                                                                    │    risk-ops-service     │
                                                                    │     (FastAPI :8001)     │
                                                                    └─────────────┬───────────┘
                                                                                  │
                                                       10. Tenant-Isolated WS (/ws/alerts?merchant_id=...)
                                                           + REST Snapshot Recovery (/api/v1/ops/alerts)
                                                                                  ▼
                                                                    ┌─────────────────────────┐
                                                                    │      dashboard-ui       │
                                                                    │     (Next.js :3000)     │
                                                                    └─────────────────────────┘
```

---

## 3. Target Monorepo Directory Structure

```text
sentinel-pay/
├── docker-compose.yml
├── README.md
├── proto/
│   └── fraud.proto                        # gRPC contract (uses int64 amount_cents)
├── scripts/
│   └── chaos_and_load_tests.py            # Automated verification of the 5 CV scenarios
├── services/
│   ├── fraud-engine-service/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── init_model.py                  # Pre-trains calibrated IsolationForest artifact
│   │   ├── rules_engine.py                # Versioned deterministic rules (v1)
│   │   ├── ml_scorer.py                   # Optional anomaly scorer plug-in
│   │   └── server.py                      # Pure gRPC Servicer (:50051)
│   ├── payment-service/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── alembic.ini
│   │   ├── migrations/                    # Alembic migrations for `payment_core` schema
│   │   ├── main.py                        # FastAPI app, lifespan, public & internal routes
│   │   ├── auth.py                        # Merchant API Key & Operator Role header middleware
│   │   ├── models.py                      # SQLAlchemy models (Payment, IdempotencyRecord, Outbox, Audit)
│   │   ├── idempotency.py                 # Stripe-style DB + Redis idempotency & SHA-256 payload hashing
│   │   ├── redis_velocity.py              # Redis Lua script execution & customer freeze checks
│   │   ├── payment_provider.py            # PaymentProvider interface + SimulatedBankProvider
│   │   ├── grpc_client.py                 # gRPC client with 15ms deadline & fallback rules
│   │   └── workers.py                     # Outbox Relay Worker & Provider Reconciliation Worker
│   └── risk-ops-service/
│       ├── Dockerfile
│       ├── requirements.txt
│       ├── alembic.ini
│       ├── migrations/                    # Alembic migrations for `risk_ops_read` schema
│       ├── main.py                        # FastAPI app, REST snapshot API, WebSocket server
│       ├── consumer.py                    # Idempotent Kafka consumer (`processed_events` dedup)
│       ├── ws_manager.py                  # Tenant-isolated WebSocket rooms (`merchant_id`)
│       └── command_client.py              # Calls `payment-service` internal APIs for operator actions
└── frontend/
    ├── Dockerfile
    ├── package.json
    └── src/                               # Next.js Split-Screen UI
```

---

## 4. Detailed Engineering Specifications & Code Blueprints

### 4.1 Multi-Tenancy, Money Representation & RBAC Headers
1. **Integer Minor Units (No Floats):** All monetary values across REST JSON, PostgreSQL (`BIGINT`), Redis Lua scripts, and gRPC Protobuf are represented in **integer minor units (`amount_cents`)** paired with an ISO-4217 **`currency`** code (e.g., `$150.00 USD` = `15000`, `100.50 EGP` = `10050`).
2. **Merchant Isolation (`merchant_id`):**
   * Pre-seeded merchants in DB: `merch_talabat` (API Key: `sk_test_talabat_111`) and `merch_noon` (API Key: `sk_test_noon_222`).
   * Every checkout request requires `X-API-Key`, which resolves `merchant_id`.
3. **Lightweight Operator RBAC & Auditability:**
   * Instead of building heavy login screens, operator requests pass headers: `X-Merchant-Id`, `X-Operator-Id` (e.g., `op_omar`), and `X-Operator-Role` (`viewer`, `analyst`, `admin`).
   * `viewer`: Read-only access to dashboard and WebSockets.
   * `analyst`: Can execute `Capture` and `Void` on `REVIEW` transactions.
   * `admin`: Can execute `Freeze Customer & Retroactive Clawback`.

---

### 4.2 The gRPC Contract (`proto/fraud.proto`)

```protobuf
syntax = "proto3";

package fraud.v1;

service FraudDetector {
  rpc EvaluateTransaction (FraudRequest) returns (FraudResponse);
}

message FraudRequest {
  string transaction_id = 1;
  string merchant_id = 2;
  string customer_id = 3;
  int64 amount_cents = 4;             // Strictly integer minor units
  string currency = 5;                // e.g., "EGP", "USD"
  string country = 6;                 // ISO country code, e.g., "EG"
  int32 tx_count_5m = 7;              // Tenant+Customer+Currency scoped from Redis Lua
  int64 total_amount_cents_5m = 8;    // Sum of amounts in last 5m from Redis Lua
  bool enable_ml_scoring = 9;         // Feature flag to compare Rules-Only vs. Hybrid ML
}

message FraudResponse {
  int32 risk_score = 1;               // 0 to 100
  string risk_decision = 2;           // "ALLOW", "REVIEW", "BLOCK"
  repeated string reasons = 3;        // Triggered rule names & explanations
  string engine_version = 4;          // e.g., "rules_v1.0+iforest_v1.0"
  double inference_latency_ms = 5;
}
```

---

### 4.3 Redis Atomic Lua Script for Velocity & Customer Freeze (`payment-service/redis_velocity.py`)

To avoid pulling full transaction lists over the network into Python on every request, Redis executes a pre-loaded **Lua Script** atomically in $< 0.5\text{ ms}$. Keys are strictly scoped by `{merchant_id}:{customer_id}:{currency}`.

```python
VELOCITY_LUA_SCRIPT = """
-- KEYS[1]: freeze_key      -> freeze:{merchant_id}:{customer_id}
-- KEYS[2]: zset_key        -> velocity:{merchant_id}:{customer_id}:{currency}
-- ARGV[1]: now_ms          -> current epoch milliseconds
-- ARGV[2]: window_start_ms -> now_ms - 300000 (5 minutes)
-- ARGV[3]: tx_id
-- ARGV[4]: amount_cents

-- 1. Check if customer is frozen for this merchant
if redis.call('EXISTS', KEYS[1]) == 1 then
    return {-1, 0}
end

-- 2. Evict expired entries outside the 5-minute sliding window
redis.call('ZREMRANGEBYSCORE', KEYS[2], 0, ARGV[2])

-- 3. Sum existing amounts inside Redis RAM
local entries = redis.call('ZRANGE', KEYS[2], 0, -1)
local total_cents = tonumber(ARGV[4])
for _, entry in ipairs(entries) do
    local sep = string.find(entry, ":")
    if sep then
        total_cents = total_cents + tonumber(string.sub(entry, sep + 1))
    end
end

-- 4. Record current attempt and set 10-minute TTL
redis.call('ZADD', KEYS[2], ARGV[1], ARGV[3] .. ":" .. ARGV[4])
redis.call('EXPIRE', KEYS[2], 600)

local tx_count = #entries + 1
return {tx_count, total_cents}
"""
```
* If `tx_count == -1`, the customer is frozen (`CUSTOMER_FROZEN`), and `payment-service` immediately blocks the transaction without calling `fraud-engine-service` or the bank.

---

### 4.4 Separation of Risk Decision, Payment State Machine & Simulated Provider

#### A. Orthogonal Enums & Allowed State Transitions
* **`risk_decision`:** `ALLOW` | `REVIEW` | `BLOCK`
* **`payment_status`:** `CREATED` | `AUTHORIZED` | `AUTHORIZED_PENDING_RECONCILIATION` | `CAPTURED` | `VOIDED` | `REFUNDED` | `FAILED`

```text
                       ┌── (risk=BLOCK) ──────────────────────────────────► FAILED (Blocked by Risk)
                       │
                       ├── (risk=ALLOW) ──► Provider.authorize_and_capture()
                       │                         ├── Success ─────────────► CAPTURED ──(Clawback/Refund)──► REFUNDED
                       │                         ├── Insufficient Funds ──► FAILED
CREATED (Initial) ─────┤                         └── Network Timeout ─────► AUTHORIZED_PENDING_RECONCILIATION
                       │
                       └── (risk=REVIEW) ─► Provider.authorize_only()
                                                 ├── Success ─────────────► AUTHORIZED (On-Hold for Operator)
                                                 │                               ├── Operator Approve ────► CAPTURED
                                                 │                               └── Operator Reject/Clawback ► VOIDED
                                                 └── Declined ────────────► FAILED
```

#### B. Concurrency-Safe State Transitions (Optimistic Locking)
To prevent two operators from simultaneously clicking `Capture` and `Void` on the same `AUTHORIZED` payment:
```sql
UPDATE payment_core.payments
SET payment_status = :new_status,
    version = version + 1,
    updated_at = NOW()
WHERE id = :payment_id
  AND merchant_id = :merchant_id
  AND payment_status = 'AUTHORIZED'
  AND version = :expected_version;
```
If `rowcount == 0`, return `HTTP 409 Conflict ("Payment state already modified by another operator")`.

#### C. Simulated Payment Provider Interface (`payment_provider.py`)
Supports deterministic chaos injection via the `card_token` or `simulate_behavior` request field:
* `tok_visa_normal`: Succeeds in $8\text{ ms}$ (`provider_tx_id = "prov_..."`).
* `tok_declined_funds`: Returns `ProviderResponse(success=False, error_code="INSUFFICIENT_FUNDS")`.
* `tok_timeout_after_charge`: The simulated bank **records the charge internally** in its own in-memory ledger (keyed by `idempotency_key`), waits $25\text{ ms}$, and raises `ProviderTimeoutError`.
  * **Reconciliation Recovery:** `payment-service` catches `ProviderTimeoutError`, sets status to `AUTHORIZED_PENDING_RECONCILIATION`, and a background task calls `provider.get_transaction_by_idempotency_key(idem_key)` $3\text{ seconds}$ later to resolve the status to `CAPTURED` or `AUTHORIZED` without double-charging!

---

### 4.5 Crash-Safe Idempotency in PostgreSQL (`payment-service/idempotency.py`)

Every `POST /api/v1/payments` request computes a canonical `SHA-256` hash (`request_hash`) of `(customer_id, amount_cents, currency, country)`.

```sql
CREATE TABLE payment_core.idempotency_records (
    merchant_id VARCHAR(64) NOT NULL,
    idempotency_key VARCHAR(128) NOT NULL,
    request_hash VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL, -- 'IN_PROGRESS', 'COMPLETED'
    http_status_code INT,
    response_body JSONB,
    locked_until TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (merchant_id, idempotency_key)
);
```
1. **Atomic Claim:** Execute `INSERT INTO payment_core.idempotency_records ... ON CONFLICT (merchant_id, idempotency_key) DO NOTHING RETURNING *`.
2. **If Conflict (Row Already Exists):**
   * Fetch existing row.
   * **Payload Tampering Check:** If `existing.request_hash != current_request_hash`, immediately raise `HTTP 422 Unprocessable Entity ("Idempotency key reused with different request parameters")`.
   * If `existing.status == 'COMPLETED'`, return `existing.response_body` with `existing.http_status_code` and header `X-Idempotent-Replay: true`.
   * If `existing.status == 'IN_PROGRESS'` and `NOW() < existing.locked_until`, raise `HTTP 409 Conflict ("Request currently in progress")`. (If `NOW() >= existing.locked_until`, a previous worker crashed mid-flight; recover by inspecting `payment_core.payments`).
3. **Finalization:** Inside the final database transaction that saves the `Payment` and `OutboxEvent`, update `idempotency_records` to `status = 'COMPLETED'` and store `response_body`.

---

### 4.6 Consistent Fraud Engine Rules & Calibrated Anomaly Detection (`fraud-engine-service/`)

All thresholds in the code, tests, and demo buttons are mathematically aligned:

#### A. Versioned Deterministic Rules (`rules_engine.py` — Version `rules_v1.0`)
```python
def evaluate_rules_v1(req) -> tuple[int, list[str]]:
    score = 0
    reasons = []

    # Rule 1: High Single Transaction Amount (> 20,000.00 EGP/USD -> 2,000,000 cents)
    if req.amount_cents > 2_000_000:
        score += 55
        reasons.append(f"RULE_HIGH_AMOUNT: {req.amount_cents / 100:,.2f} {req.currency} > 20,000.00")

    # Rule 2: Rapid Transaction Count Velocity (>= 4 transactions in 5 minutes)
    # Catches the attacker on the 4th attempt!
    if req.tx_count_5m >= 4:
        score += 50
        reasons.append(f"RULE_VELOCITY_COUNT: {req.tx_count_5m} transactions in 5m window (limit: 3)")

    # Rule 3: Cumulative Spend Velocity (> 30,000.00 in 5 minutes -> 3,000,000 cents)
    if req.total_amount_cents_5m > 3_000_000:
        score += 45
        reasons.append(f"RULE_VELOCITY_SUM: {req.total_amount_cents_5m / 100:,.2f} {req.currency} cumulative in 5m")

    # Rule 4: High-Risk Foreign Geo-Location
    if req.country.upper() not in ("EG", "US", "AE", "SA"):
        score += 40
        reasons.append(f"RULE_FOREIGN_GEO: Country '{req.country}' outside allowed domestic list")

    return min(score, 100), reasons
```

#### B. Calibrated Multi-Feature ML Anomaly Detection (`init_model.py` & `ml_scorer.py`)
To ensure `IsolationForest` genuinely detects **2D/3D Threshold Gaming** (rather than flagging any medium amount on the 1st transaction):
* **Training Distribution (`init_model.py`):** Trained on two realistic normal clusters:
  1. *Everyday purchases:* `amount_cents` between `5,000` ($50) and `200,000` ($2,000) with `tx_count_5m` in `{1, 2, 3}`.
  2. *Occasional high-ticket purchases:* `amount_cents` up to `1,900,000` ($19,000), **strictly with `tx_count_5m == 1`** and `total_amount_cents_5m == amount_cents`.
* **Result:** A single purchase of `$9,500` (`950,000 cents`, `tx_count_5m = 1`) is classified as **Normal (`ALLOW`)** by both the Rules and the ML model! However, when an attacker executes **3 rapid purchases of `$9,500`** (`tx_count_5m = 3`, `total_amount_cents_5m = 2,850,000`), it stays **under** all static rules (`amount < 20k`, `count < 4`, `sum < 30k`), but falls into the empty anomaly region of the `IsolationForest` decision function, adding `+45` risk points and triggering `REVIEW`!

---

### 4.7 Transactional Outbox, Idempotent Kafka Consumer & Read-Model Projection

#### A. Outbox Event Payload (`payment_core.outbox_events`)
Every state change in `payment-service` (initial checkout, operator capture, operator void, retroactive clawback, reconciliation) inserts an immutable event with a `UUIDv4` `event_id`:
```json
{
  "event_id": "evt_9a8b7c6d",
  "event_type": "PAYMENT_EVALUATED",
  "merchant_id": "merch_talabat",
  "payment_id": "pay_1001",
  "customer_id": "cust_omar_99",
  "amount_cents": 2500000,
  "currency": "EGP",
  "risk_score": 95,
  "risk_decision": "BLOCK",
  "payment_status": "FAILED",
  "reasons": ["RULE_VELOCITY_COUNT: 4 transactions in 5m window"],
  "version": 1,
  "occurred_at": "2026-10-28T12:00:00Z"
}
```

#### B. Idempotent Kafka Consumer (`risk-ops-service/consumer.py`)
Because the Outbox Relay guarantees **at-least-once** delivery, `risk-ops-service` prevents duplicate processing by committing the `event_id` and updating its read-model projection in a single database transaction **before** committing the Kafka offset:

```sql
-- Inside Schema: risk_ops_read
CREATE TABLE risk_ops_read.processed_events (
    event_id VARCHAR(64) PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE risk_ops_read.payment_projections (
    payment_id VARCHAR(64) PRIMARY KEY,
    merchant_id VARCHAR(64) NOT NULL,
    customer_id VARCHAR(64) NOT NULL,
    amount_cents BIGINT NOT NULL,
    currency VARCHAR(8) NOT NULL,
    country VARCHAR(8) NOT NULL,
    risk_score INT NOT NULL,
    risk_decision VARCHAR(16) NOT NULL,
    payment_status VARCHAR(48) NOT NULL,
    reasons JSONB NOT NULL,
    version INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
```

---

### 4.8 True Financial Clawback (`VOID` vs. `REFUND`) & Customer Freeze

When an `admin` clicks **`[Freeze Customer & Clawback Recent Tx]`** in `dashboard-ui`:
1. `risk-ops-service` forwards the command to `payment-service` (`POST /internal/v1/merchants/{merchant_id}/customers/{customer_id}/clawback`) with operator audit metadata.
2. `payment-service` sets `freeze:{merchant_id}:{customer_id} = "1"` in Redis (blocking all future attempts).
3. `payment-service` queries all transactions for `(merchant_id, customer_id)` in the last 15 minutes where `payment_status IN ('AUTHORIZED', 'CAPTURED')` with `FOR UPDATE`:
   * For every **`AUTHORIZED`** transaction (money held, not yet settled): calls `provider.void(payment_id)` and transitions `payment_status -> VOIDED`.
   * For every **`CAPTURED`** transaction (money already captured): calls `provider.refund(payment_id)` and transitions `payment_status -> REFUNDED`.
4. Inserts an `AuditLog` record (`operator_id`, `action='CUSTOMER_FREEZE_AND_CLAWBACK'`, `affected_payment_ids`, `reason`) and emits `PAYMENT_CLAWBACK_EXECUTED` events to the Outbox so all connected WebSocket dashboards update immediately.

---

## 5. The 5 Reproducible Engineering Demos (For CV & Interviews)

Both the `dashboard-ui` Simulator Panel and `scripts/chaos_and_load_tests.py` implement these 5 verifiable engineering proofs:

1. **Demo 1: Parallel Idempotency & Payload Tampering Protection**
   * Fires 10 concurrent `POST /api/v1/payments` requests with the exact same `Idempotency-Key`. Proves via DB query that **exactly 1 payment row** is created and 1 bank charge occurs. Then fires the same key with a modified `amount_cents` and verifies `HTTP 422`.
2. **Demo 2: Velocity Attack & Differentiated Financial Clawback (`VOID` vs. `REFUND`)**
   * Executes 3 rapid `$400` transactions (`ALLOW` -> `CAPTURED`), followed by a 4th `$25,000` transaction (`BLOCK` -> `FAILED`, caught by `RULE_VELOCITY_COUNT >= 4`). Operator clicks **Freeze & Clawback** -> the first 3 `CAPTURED` transactions transition to `REFUNDED`, and a 5th attempt is instantly rejected by the Redis Freeze check.
3. **Demo 3: Threshold Gaming Caught by Calibrated ML (`Rules-Only` vs. `Hybrid ML`)**
   * Sends 3 rapid transactions of `$9,500` (`total = $28,500`, staying below the `$20k` single limit, `4 tx` count limit, and `$30k` sum limit). Shows that with `enable_ml_scoring=False` it passes, whereas with `enable_ml_scoring=True` the `IsolationForest` catches the 2D anomaly and places it in `REVIEW` (`AUTHORIZED`).
4. **Demo 4: Operator Race Condition Prevention (Optimistic Locking)**
   * Two concurrent operator requests attempt `POST /capture` and `POST /void` on the same `AUTHORIZED` transaction at the exact same millisecond. One succeeds (`HTTP 200`), and the second deterministically fails (`HTTP 409 Conflict`).
5. **Demo 5: Fault Tolerance (Kafka Outage Recovery & gRPC Fallback)**
   * Pauses the Kafka container (`docker compose pause kafka`), processes 5 payments (all succeed via PostgreSQL Outbox), then unpauses Kafka and verifies that all 5 events stream into the WebSocket dashboard with zero data loss and zero duplicate projections.

---

## 6. Phased Implementation Plan (Execution Order for Coding Agent)

Execute the implementation strictly in these 4 sequential milestones:

* **Milestone 1: Core Data Ownership, Multi-Tenancy, Idempotency & Simulated Provider (`payment-service` + `fraud-engine-service` Rules)**
  * Set up `docker-compose.yml` (`postgres`, `redis`, `kafka` KRaft).
  * Create `proto/fraud.proto` (`int64 amount_cents`) and implement `fraud-engine-service` with `rules_v1.0` first.
  * Create Alembic migrations for `payment_core` schema (`payments`, `idempotency_records`, `outbox_events`, `audit_logs`).
  * Implement Merchant API Key auth, Stripe-style DB Idempotency, Redis Lua Velocity script, `SimulatedBankProvider` (including timeout reconciliation), and gRPC client with $15\text{ ms}$ timeout fallback.
* **Milestone 2: Transactional Outbox, Idempotent Kafka Consumer & Risk Ops Read-Model (`risk-ops-service`)**
  * Implement `OutboxRelayWorker` in `payment-service`.
  * Create Alembic migrations for `risk_ops_read` schema (`processed_events`, `payment_projections`).
  * Implement idempotent Kafka consumer, tenant-isolated WebSocket manager (`/ws/alerts?merchant_id=...`), REST state recovery endpoint (`GET /api/v1/ops/alerts`), and internal command forwarding (`Capture`, `Void`, `Clawback`).
* **Milestone 3: Split-Screen Next.js Dashboard (`dashboard-ui`)**
  * Build the Merchant/Role switcher bar, Checkout & Scenario Simulator buttons, Live KPI bar, Tenant-isolated Alert Feed with 15-minute Attack Chains, and Audit Log viewer.
* **Milestone 4: Calibrated ML Plug-in, Chaos Scripts & Latency Benchmarks**
  * Add `init_model.py` (`IsolationForest`) to `fraud-engine-service` and wire the `enable_ml_scoring` toggle.
  * Create `scripts/chaos_and_load_tests.py` to automatically run and print p95/p99 latency metrics and verify the 5 CV scenarios.