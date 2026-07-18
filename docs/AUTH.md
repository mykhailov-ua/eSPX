# Auth: Security, Argon2id, and Chaos Tests

The `cmd/auth` service is a gRPC server (cold path). It handles registration, login, PASETO sessions, and API keys. Lockout state is stored on Redis shard 0; credentials live in Postgres.

---

## 1. Attack Surface

*   **Credential Stuffing.** Login attempts via the gRPC login path. Primary risk: CPU exhaustion from the Argon2id algorithm.
*   **User Enumeration.** Inferring whether an email exists from response timing. Mitigated via Dummy Hashing (computing a hash for non-existent users).
*   **API Key Brute Force.** Mass key verification. Protected by skipping Argon2 on database misses and rate limits at the management service layer.

---

## 2. Argon2id Usage

Passwords and secrets are hashed with Argon2id. Verification is resource-intensive (CPU and RAM).

**Default parameters:**
*   Memory: 64 MiB.
*   Iterations: 3.
*   Parallelism: 4 threads.

A single verification takes 50–200 ms. To prevent denial of service (DoS), a `cryptoSem` semaphore limits the number of concurrent computations.

---

## 3. Protection Layers (Login)

Login verification chain:
1.  Email format validation.
2.  **Redis AllowIP.** Limit: 20 requests per minute from a single IP.
3.  **Redis Lockout.** Account lockout (5 attempts per IP+Email pair within 10 minutes).
4.  **Postgres GetUser.** Fetch the user or run Dummy Hash.
5.  **cryptoSem.** Check CPU resource availability.
6.  **VerifyPassword.** Run the Argon2id algorithm.

---

## 4. IP Addresses and Trusted Proxies

The service extracts the client IP from gRPC metadata. `X-Real-IP` and `X-Forwarded-For` headers are honored only when the request comes from a trusted node (Management or Nginx) listed in `TrustedProxies`.

---

## 5. Monitoring

*   `auth_login_attempts_total`. Login attempts broken down by reason: `ratelimit`, `locked`, `invalid_credentials`.
*   `auth_token_errors_total`. Token verification and refresh errors.
*   Metrics are exposed on port 9091.

---

## 6. Chaos Tests (Infrastructure)

CI includes scenarios to verify behavior during failures:
*   **redis_terminate.** Verify login lockout when Redis is unavailable.
*   **postgres_terminate.** Verify login failure when the database is unavailable.
*   **redis_stop_start_recovery.** Automatic recovery of token verification after Redis restart.

---

## 7. Priority Fixes

1.  **P0.** Pass the real client IP from Management into gRPC metadata (currently all requests appear as 127.0.0.1).
2.  **P0.** Apply the `cryptoSem` semaphore to all Argon2 entry points (currently only Login).
3.  **P1.** Rate limiting for API key verification.
