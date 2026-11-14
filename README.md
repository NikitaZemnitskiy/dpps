# Distributed Payment Processing System (DPPS)

A payment store and statistics engine running as a cluster of three Spring Boot nodes, each with an embedded
Apache Ignite 2 server node. Payments live in a partitioned in-memory data grid (one backup per entry); statistics
are computed with a partition-aware MapReduce across the cluster. Access is secured with Keycloak (OAuth2/JWT).

## Tech stack

- **Java 17**, **Spring Boot 3.5.11**
- **Apache Ignite 2.17.0**: embedded server nodes, partitioned cache with one backup, key-value and scan API only (no SQL)
- **Keycloak 24.0.2**: OAuth2 access tokens with realm roles
- **Nginx**: round-robin load balancer in front of the nodes
- **Docker Compose**: the whole cluster on one machine

## Quick start

```bash
docker compose up -d --build
```

This starts five containers: `keycloak` (port 8090), `app-node-1..3` and `nginx` (port 80). Once
`docker compose ps` shows the three nodes as `healthy` (about a minute), the cluster has formed:

```bash
docker compose logs app-node-1 | grep "Topology snapshot"   # ... servers=3, clients=0, state=ACTIVE ...
```

Get an access token:

```bash
TOKEN=$(curl -s -X POST http://localhost:8090/realms/payment-system/protocol/openid-connect/token \
  -d client_id=payment-client -d grant_type=password \
  -d username=user -d password=user | jq -r .access_token)
```

### Users and roles

| User     | Password | Realm roles                       | May                                        |
|----------|----------|-----------------------------------|--------------------------------------------|
| `user`   | `user`   | `payments-read`, `payments-write` | everything                                 |
| `viewer` | `viewer` | `payments-read`                   | read payments and statistics (403 otherwise) |

The API accepts a token only if its signature, expiry, issuer (`http://localhost:8090/realms/payment-system`) and
audience (`payment-service`) are valid. Keycloak puts that issuer into every token, whichever host name you use to
reach it. These users, passwords and the password grant are a local demo setup; see [Design notes](#design-notes).

## API

All endpoints except `/actuator/health` require `Authorization: Bearer <token>`.

**Date-times** use one format everywhere (CSV files and query parameters): an ISO-8601 local date-time with
optional seconds and fraction, such as `2026-02-20T12:00`, `2026-02-20T12:00:30` or `2026-02-20T12:00:30.250`.
A space is accepted instead of `T`; years must be between 1 and 9999. Time zones and offsets are rejected; the
system works with local date-times. In responses, date-times always include the seconds (`2026-02-20T12:00:00`).
A parameter given with an empty value (`from=`) is an error, not "no bound".

**Time ranges include both bounds** and compare real timestamps: `to=2026-02-20T23:59` means 23:59:00, so use
`T23:59:59` (or the next day's `T00:00`) to cover a whole day.

**Errors** are JSON: `{"status": 400, "error": "Bad Request", "message": "..."}`.

| Status | When |
|--------|------|
| 400 | invalid parameters (unparseable date-time, `from` after `to`, range too long, ...), CSV without required columns or with broken CSV syntax |
| 401 | missing, expired or otherwise invalid token |
| 403 | the token lacks the role the endpoint needs |
| 413 | file larger than 500 MB |
| 415 | upload that is not `multipart/form-data` |
| 503 | the cluster is temporarily unavailable (a node left mid-operation, timeout; comes with `Retry-After`), or payments were lost and the loss hasn't been acknowledged yet (see [Durability](#durability)) |
| 507 | the cluster is running out of memory; the upload was refused or stopped |
| 500 | unexpected error; the message carries a reference to the log entry, never internal details |

### Upload payments: `POST /api/payments/upload` (`payments-write`)

```bash
curl -X POST http://localhost/api/payments/upload -H "Authorization: Bearer $TOKEN" \
  -F "file=@postman/test-data/payments-main.csv"
```

The CSV needs a header row with the columns `DateTime, Sender, Receiver, Amount, ID`, in any order and any case;
`Value` is accepted instead of `Amount`, and extra columns are ignored. The file is processed as a stream, so it is
never held in memory as a whole.

Every row is validated. Invalid rows are skipped and counted; the rest is stored:

```json
{
  "successfullyLoaded": 1000,
  "rejected": 4,
  "errors": { "invalid_datetime": 1, "invalid_value": 1, "missing_sender": 2 }
}
```

| Error code | Meaning |
|------------|---------|
| `missing_<field>` | empty `id`, `sender`, `receiver`, `datetime` or `value` |
| `invalid_datetime` | not a valid date-time in the format above (e.g. `2026-02-30T10:00`, `20.02.2026`) |
| `invalid_value` | not a positive amount in plain decimal notation (`-5`, `0`, `1e3`, `1,000`, `abc`) |
| `invalid_id`, `invalid_sender`, `invalid_receiver` | longer than 255 characters; bank names also must not contain `->` |
| `invalid_csv_row` | the row has fewer columns than the header (or extra non-empty ones) |

`errors` counts problems, so a row with two problems adds to two codes but to `rejected` only once.
Amounts are stored as exact decimals.

- **Payments are upserted by ID.** A row whose ID already exists (in the cluster or earlier in the same file)
  replaces the earlier payment; `successfullyLoaded` counts every stored row. Uploading the same file twice is
  therefore harmless.
- **The whole file is rejected (400)** if required columns are missing from the header, or if the CSV syntax is
  broken (an unterminated quote, or a line longer than 64K characters). In the second case, valid rows before the
  broken line are stored, and the message gives the line number and how many payments were stored.
- **Uploads stop before the cluster runs out of memory (507)**, see [Capacity](#memory-and-capacity). The
  message says how many payments were stored.

In every error case it is safe to fix the cause and upload the same file again.

### Get payments: `GET /api/payments?from=...&to=...` (`payments-read`)

```bash
curl "http://localhost/api/payments?from=2026-02-20T00:00&to=2026-02-20T23:59:59" -H "Authorization: Bearer $TOKEN"
```

```json
[{"id":"1","dateTime":"2026-02-20T12:00:00","sender":"Bank A","receiver":"Bank B","value":100}]
```

Both bounds are required and the range may span at most 7 days. The result is streamed as it is read from the
cluster, so a large range never has to fit into memory; the order is unspecified. A stream has no time limit, and
each node serves up to 64 streams at a time. If reading fails before any data was sent, the response is a normal
error (e.g. 503). If it fails later, the connection is aborted, so clients see a broken transfer, never a list that
merely looks complete.

### Delete payments: `DELETE /api/payments` (`payments-write`)

```bash
# delete a time range
curl -X DELETE "http://localhost/api/payments?from=2026-02-20T00:00&to=2026-02-20T23:59:59" -H "Authorization: Bearer $TOKEN"
# delete everything
curl -X DELETE http://localhost/api/payments -H "Authorization: Bearer $TOKEN"
```

Response: `{"deletedCount": 5}`. Give both bounds or neither. A request with only one bound is rejected (400),
because it is far more likely a mistake than a request to delete everything.

### Statistics: `GET /api/statistics` (`payments-read`)

| Parameter | Required | Values |
|-----------|----------|--------|
| `aggregation` | yes | `BY_DATE`, `BY_BANK` or `BY_CONNECTION` |
| `metrics` | yes | one or more of `GENERAL`, `VALUE`, `DATETIME`: repeat the parameter or separate with commas |
| `from`, `to` | no | inclusive bounds; either may be omitted for an open-ended range |

```bash
curl "http://localhost/api/statistics?aggregation=BY_DATE&metrics=GENERAL,VALUE" -H "Authorization: Bearer $TOKEN"
```

```json
{
  "data": {
    "2026-02-20": { "general": {"count": 5}, "value": {"min": 50, "max": 200, "sum": 500, "average": 100} },
    "2026-02-21": { "general": {"count": 10}, "value": {"min": 50, "max": 300, "sum": 1350, "average": 135} }
  }
}
```

Groups are sorted by key, and only the requested metric sections appear.

| Aggregation | Group key | Meaning |
|-------------|-----------|---------|
| `BY_DATE` | `yyyy-MM-dd` | payments of that calendar day |
| `BY_BANK` | bank name | every payment has two sides: `-amount` for the sender and `+amount` for the receiver. `count` is the number of payments the bank took part in, and `sum` is its net position. |
| `BY_CONNECTION` | `Sender -> Receiver` | payments in that direction (`A -> B` and `B -> A` are different groups); bank names can't contain `->`, so keys are unambiguous |

| Metric | Fields |
|--------|--------|
| `GENERAL` | `count` |
| `VALUE` | `min`, `max`, `sum` (exact) and `average` (rounded half-up to 4 decimals) |
| `DATETIME` | `min`, `max` and `average` (the mean timestamp, to the second) |

## Architecture

```
                     +----------------------+
  client --HTTP-->   |  nginx (round robin) |
                     +----------------------+
                        |        |        |
                        v        v        v
               +------------+------------+------------+
               | app-node-1 | app-node-2 | app-node-3 |   Spring Boot + embedded Ignite server node
               +------------+------------+------------+
                 partitioned "payments" cache, 1 backup per entry (FULL_SYNC)

  Keycloak <-- signing keys (JWKS) -- every node        clients get their tokens from Keycloak directly
```

- **Every application instance is a full Ignite server node.** Payments are spread over 1,024 partitions; each
  partition has a primary and a backup copy on different nodes. A write is acknowledged only once both copies are
  written (`FULL_SYNC`), so an accepted upload survives the loss of any single node.
- **Discovery is static.** Nodes find each other through `IGNITE_DISCOVERY_ADDRESSES`; multicast is never used.
- **Statistics are a partition-aware MapReduce** (`StatisticsTask`). The task takes one consistent snapshot of which
  node is primary for which partition and sends each node a single job listing exactly its partitions, so every
  partition is counted once and backups never are. Each job scans its partitions with local, partition-scoped scan
  queries; Ignite reserves a partition while it is scanned and refuses a partition the node no longer owns. If a node
  leaves or a partition moves mid-computation, the job fails instead of returning partial data, and the whole task
  is retried on a fresh snapshot (up to 3 attempts, then 503). Partial results are merged on the requesting node.
- **Uploads** are parsed as a stream and written in batches of 1,000 with `putAll`. A memory guard checks, before
  every batch, how full the fullest node's data region is.
- **GET and DELETE** use distributed scan queries whose filter runs on the data nodes, so only matching entries
  cross the network; DELETE transfers only the keys.
- **nginx** re-resolves the node names through Docker's DNS, so a node recreated with a new IP address is found
  again without restarting nginx.

## Operations

### Configuration

The defaults live in `src/main/resources/application.yml`; the deployment overrides them with environment variables:

| Variable | Default | Purpose |
|----------|---------|---------|
| `IGNITE_INSTANCE_NAME` | `payment-ignite` | node name |
| `IGNITE_DISCOVERY_ADDRESSES` | `127.0.0.1:47500..47509` | cluster members (comma-separated `host:port-range`) |
| `IGNITE_DATA_REGION_MAX_SIZE` | `512MB` | off-heap memory for payments per node |
| `IGNITE_DATA_REGION_INITIAL_SIZE` | `64MB` | off-heap memory allocated at start |
| `IGNITE_LOCAL_HOST` | (all interfaces) | address to bind Ignite to |
| `IGNITE_WORK_DIR` | `<tmp>/dpps-ignite` | Ignite work directory |
| `KEYCLOAK_JWK_SET_URI` | `http://localhost:8090/.../certs` | where signing keys are fetched |
| `KEYCLOAK_ISSUER_URI` | `http://localhost:8090/realms/payment-system` | expected `iss` claim |
| `KEYCLOAK_AUDIENCE` | `payment-service` | expected `aud` claim |
| `JDK_JAVA_OPTIONS` | (none) | JVM heap etc.; docker-compose sets `-Xms512m -Xmx512m` |

`jvm.options` holds the JVM flags Ignite needs on Java 17 (`--add-opens ...`). The Docker image, `./mvnw test` and
`./mvnw spring-boot:run` all read this one file.

### Memory and capacity

Each node keeps its share of the payments off-heap, in a data region of `IGNITE_DATA_REGION_MAX_SIZE` (512 MB by
default). With three nodes and one backup, that holds about 3.5 million payments; one million payments use about
23% of each node's region.

An Ignite node whose data region overflows treats this as a critical failure and **halts its JVM**. Payments are
spread evenly, so one oversized upload would take down every node at once. The application therefore refuses or
stops uploads (HTTP 507) when the fullest node's region is 85% full (`dpps.upload.memory-high-watermark`); the
cluster stays fully available for reads and deletes. Delete data or give the nodes more memory to upload more.
docker-compose limits each container to 2 GB (512 MB heap, 512 MB data region, JVM overhead).

### Durability

The grid is in-memory, and the source files are the system of record. Losing any single node loses nothing: its
backups are promoted and new backups are rebuilt on the remaining nodes. Crashed nodes are restarted by Docker
(`restart: unless-stopped`) and rejoin the cluster.

Losing two nodes before the backups have been rebuilt loses the payments whose two copies were on them:

- **The cluster never answers with silently incomplete data.** The cache uses Ignite's
  `PartitionLossPolicy.READ_WRITE_SAFE`. While lost partitions exist, statistics, GET and uploads return 503 with an
  explanation, and the health details show `lostPartitions`. To recover, delete all payments
  (`DELETE /api/payments`), which acknowledges the loss, and upload the source files again.
- **If two of three nodes fail abruptly at the same moment**, the survivor can't tell whether it is the one that has
  been cut off. Ignite then declares it segmented and stops it (split-brain protection); Docker restarts it with an
  empty store. Restarting the whole cluster likewise starts empty.

### Health, shutdown, logs

- `GET /actuator/health` is public and returns `UP` only while the node's Ignite cluster is active. With a
  `payments-read` token it also shows details (node, cluster state, number of server nodes, data region usage).
  The Docker image's `HEALTHCHECK` uses it.
- `docker stop` triggers a graceful shutdown: running requests (e.g. an upload) are finished first, then the node
  leaves the cluster. `stop_grace_period` is 45 s.
- Everything, including Ignite, logs through SLF4J/Logback. Uploads and deletes are logged with the user who made
  them.

## Development

### Running without Docker

```bash
docker compose up -d keycloak     # tokens
./mvnw spring-boot:run            # one node on :8080 (jvm.options is applied automatically)
```

A second local node needs its own name and port, e.g.
`IGNITE_INSTANCE_NAME=node-2 ./mvnw spring-boot:run -Dspring-boot.run.arguments=--server.port=8081`.

### Tests

```bash
./mvnw test                     # everything
./mvnw test -Dgroups=unit       # unit tests only (no Spring, no Ignite)
./mvnw test -Dgroups=integration
```

The integration tests run against a real embedded Ignite node and include:

- a **three-node cluster** (two more Ignite nodes started in the test JVM): data distribution, no double counting
  of backups, and statistics that stay exact while nodes leave;
- **security with real signed tokens**, validated against a local JWK set: signature, expiry, issuer, audience and
  roles.

Tests use their own Ignite ports (48500+) and never join a cluster running on the same machine. CI runs the tests and
builds the Docker image on every push (`.github/workflows/ci.yml`).

### Postman

`postman/Dpps.postman_collection.json` holds all requests; `postman/Local.postman_environment.json` sets
`keycloakUrl` and `dppsUrl` for the Docker setup. Import both, select the **Local** environment and run
**Auth → Get token (user)** first; it stores the token for the other requests. **Get token (viewer)** switches to
the read-only user. The upload requests use the files in `postman/test-data/`. If Postman can't find them, select
them again in the request body.

### Load testing: CSV generator

```bash
./mvnw -q test-compile exec:java -Dexec.classpathScope=test \
  -Dexec.mainClass=com.zemnitskiy.dpps.util.CsvGenerator \
  -Dexec.args="-n 1000000 -o payments-1m.csv"
```

| Option | Description | Default |
|--------|-------------|---------|
| `-n` | number of rows | 1000 |
| `-o` | output file | `payments-generated.csv` |
| `--banks` | number of banks | 10 |
| `--min` / `--max` | amount range | 1.00 / 10000.00 |
| `--start` / `--end` | date range (`yyyy-MM-dd`) | 2026-01-01 / 2026-12-31 |
| `--seed` | random seed for reproducible files | random |

For reference: in a test run of the Docker cluster on one 4-core machine, a one-million-row file (47 MB) was
stored in about 13 s through nginx, and `BY_DATE` statistics over it took about 2 s.

## Design notes

- **Exact types.** Amounts are `BigDecimal`, date-times are `LocalDateTime`. Range filters, grouping and averages
  never depend on how a file spelled its values, and sums are exact to the cent.
- **Key-value and scan API only.** GET and DELETE scan the cache with a filter that runs on the data nodes. This is
  linear in the number of stored payments; if queries by time became hot, a time-bucketed index cache (or Ignite's
  SQL indexes) would be the next step.
- **Uploads are not transactional** (a 500 MB file can't be one transaction). They are idempotent instead: payments
  are keyed by ID, and every error message says what was stored, so re-uploading is always safe.
- **Security.** Every endpoint needs a specific realm role, and everything not explicitly allowed is denied. Tokens
  are checked for issuer and audience, not just signature. The password grant, the demo users and the
  `admin/admin` Keycloak console (`KEYCLOAK_ADMIN` / `KEYCLOAK_ADMIN_PASSWORD`) are a local setup. A real deployment
  would use the authorization-code flow, proper secrets and HTTPS.
