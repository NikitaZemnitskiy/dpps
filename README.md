# Distributed Payment Processing System (DPPS)

A distributed payment processing and statistics system built with Spring Boot and Apache Ignite 2. The application operates as a cluster of 3 nodes with an in-memory partitioned data grid, providing REST APIs for payment management and MapReduce-based statistical aggregations.

## Tech Stack

- **Java 17** + **Spring Boot 3.5.11**
- **Apache Ignite 2.17.0** - In-memory data grid (Partitioned mode, 1 backup)
- **Keycloak 24.0.2** - OAuth2 SSO authentication
- **Nginx** - Round-robin load balancer
- **Docker Compose** - Container orchestration

## Quick Start

### 1. Start the cluster

```bash
docker-compose up -d --build
```

This starts 5 services:
- `keycloak` - Authentication server (port 8090)
- `app-node-1`, `app-node-2`, `app-node-3` - Application cluster nodes
- `nginx` - Load balancer (port 80)

### 2. Verify the cluster

Wait ~30 seconds for all nodes to start, then check the logs:

```bash
docker-compose logs app-node-1 | grep "hosts="
```

You should see `hosts=3, servers=3` confirming all nodes formed a cluster.

### 3. Get an authentication token

```bash
TOKEN=$(curl -s -X POST 'http://localhost:8090/realms/payment-system/protocol/openid-connect/token' \
  --header 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'client_id=payment-client' \
  --data-urlencode 'username=user' \
  --data-urlencode 'password=user' \
  --data-urlencode 'grant_type=password' | jq -r '.access_token')
```

## API Reference

All endpoints require a valid JWT token in the `Authorization: Bearer <token>` header.

### Payment Management

#### Upload CSV

```bash
curl -X POST http://localhost/api/payments/upload \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@payments.csv"
```

CSV format: `DateTime,Sender,Receiver,Amount,ID` (header row required). Invalid rows are skipped with error counts in the response.

Response example:
```json
{"successfullyLoaded": 1000, "errors": {"missing_sender": 15, "missing_value": 5}}
```

#### Get Payments (time range required, max 1 week)

```bash
curl "http://localhost/api/payments?from=2026-02-20T00:00&to=2026-02-21T23:59" \
  -H "Authorization: Bearer $TOKEN"
```

#### Delete Payments (time range optional)

```bash
# Delete all
curl -X DELETE http://localhost/api/payments \
  -H "Authorization: Bearer $TOKEN"

# Delete by range
curl -X DELETE "http://localhost/api/payments?from=2026-02-20T00:00&to=2026-02-20T23:59" \
  -H "Authorization: Bearer $TOKEN"
```

### Statistics API

Calculate aggregated statistics using MapReduce across the cluster.

**Parameters:**
- `aggregation` (required): `BY_DATE`, `BY_BANK`, or `BY_CONNECTION`
- `metrics` (required, one or more): `GENERAL`, `VALUE`, `DATETIME`
- `from` / `to` (optional): DateTime range filter

#### By Date (General + Value metrics)

```bash
curl "http://localhost/api/statistics?aggregation=BY_DATE&metrics=GENERAL&metrics=VALUE" \
  -H "Authorization: Bearer $TOKEN"
```

#### By Bank (all metrics)

```bash
curl "http://localhost/api/statistics?aggregation=BY_BANK&metrics=GENERAL&metrics=VALUE&metrics=DATETIME" \
  -H "Authorization: Bearer $TOKEN"
```

#### By Connection (General)

```bash
curl "http://localhost/api/statistics?aggregation=BY_CONNECTION&metrics=GENERAL" \
  -H "Authorization: Bearer $TOKEN"
```

## Running Tests

```bash
./mvnw test
```

Integration tests use an embedded single-node Ignite instance with mocked security.

## Architecture

```
Client -> Nginx (Round Robin) -> [App Node 1] <-> [App Node 2] <-> [App Node 3]
                                      |               |               |
                                 [Ignite Server]  [Ignite Server]  [Ignite Server]
                                      \_______________/_______________/
                                          Partitioned Cache (1 backup)
```

- Each app node is an embedded Ignite server node (thick client)
- Payment data is automatically partitioned across the cluster
- Statistics are computed via MapReduce: each node processes only its local primary data, then partial results are merged on the requesting node
- All Ignite interactions use the Key-Value API (no SQL)
