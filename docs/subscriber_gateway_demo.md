# Subscriber gateway demo

The live helper for the subscriber gateway lives in `demo/subscriber_gateway.py`:

```sh
make subscriber_gateway_demo
```

The rendered walkthrough for the same topology lives in `demo/subscriber_gateway.Rmd`:

```sh
make subscriber_gateway_rdm
```

This is the honest `ducknng` shape:

- one gateway service runs an HTTP edge, identity resolution, and subscriber lookup
- several private backend services run ordinary NNG query-session services
- the gateway resolves auth to a tenant and principal
- the gateway resolves that tenant to one enabled subscriber backend
- backend query sessions stay private to the gateway
- the gateway owns the public continuation-token contract

That is the important property at this layer: a public API edge in front of a worker plane, with query-session affinity staying on the private side.

## Execution model

All three services in the Rmd demo use `service_serialized_connection`, which gives each service its own dedicated DuckDB connection. This means a gateway route handler can make synchronous NNG calls to a sibling backend service without deadlocking, even when all services share one DuckDB runtime. The flow for a `/v1/query/start` request is:

1. Client makes HTTP request to gateway
2. Gateway service (on its dedicated connection) runs the route SQL
3. Route SQL calls `ducknng_open_query_raw(backend_url, ...)` — this blocks the gateway's connection while waiting for the NNG reply
4. Backend service (on its own dedicated connection) receives the NNG request, runs the query, returns the result
5. Gateway assembles the HTTP response

The Python demo (`demo/subscriber_gateway.py`) uses separate DuckDB processes, which is the recommended shape for production deployments with genuine per-tenant data isolation. The Rmd demo uses the in-process topology because it requires no external processes and fully demonstrates the routing, auth, session, and continuation-token contract.

## Why the gateway uses raw session helpers

HTTP route handlers execute as SQL queries. DuckDB binds table functions eagerly, so the structured client helpers:

- `ducknng_open_query(...)`
- `ducknng_fetch_query(...)`
- `ducknng_close_query(...)`
- `ducknng_cancel_query(...)`

are the ergonomic client surface, but they are the wrong surface for per-request dynamic route SQL where method inputs come from request-body columns or continuation-token columns.

The route demo therefore uses the raw synchronous session helpers:

- `ducknng_open_query_raw(...)`
- `ducknng_fetch_query_raw(...)`
- `ducknng_close_query_raw(...)`
- `ducknng_cancel_query_raw(...)`

and then inspects the returned frames with:

- `ducknng_frame_version(...)`
- `ducknng_frame_type(...)`
- `ducknng_frame_flags(...)`
- `ducknng_frame_type_name(...)`
- `ducknng_frame_name(...)`
- `ducknng_frame_payload(...)`
- `ducknng_frame_payload_text(...)`
- `ducknng_frame_error_text(...)`
- `ducknng_frame_end_of_stream(...)`

That keeps the gateway route layer generic. The route can carry tenant affinity, subscriber affinity, backend `session_id`, backend `session_token`, and fetch hints in a gateway-owned token without depending on bind-time table-function behavior.

## Topology

```text
HTTP client
    |
    v
gateway service (service_serialized_connection)
  ducknng_start_server('gateway', 'http://127.0.0.1:0/_ducknng', ...)
  ducknng_register_http_route('gateway', 'POST', '/v1/query/start', ...)
  ducknng_register_http_route('gateway', 'POST', '/v1/query/fetch', ...)
  ducknng_register_http_route('gateway', 'POST', '/v1/query/close', ...)
    |
    +--> subscriber backend: alice
    |      ducknng_start_server('subscriber_alice', 'tcp://127.0.0.1:0', ...)
    |      ducknng_set_service_execution_model('subscriber_alice', 'service_serialized_connection')
    |
    +--> subscriber backend: bob
           ducknng_start_server('subscriber_bob', 'tcp://127.0.0.1:0', ...)
           ducknng_set_service_execution_model('subscriber_bob', 'service_serialized_connection')
```

The demo uses two backends, `alice` and `bob`, each tenant's query filtered by `WHERE owner = '<tenant>'` in the client SQL. The gateway resolves bearer auth through `gateway_identities`, finds one enabled worker for the tenant through `gateway_subscribers`, and routes the query to the matching private backend. The continuation token carries the tenant id, subscriber id, backend `session_id`, backend `session_token`, and fetch hints. The backend URL stays private in the gateway tables.

## Public contract

The demo exposes three public routes:

- `POST /v1/query/start`
- `POST /v1/query/fetch`
- `POST /v1/query/close`

`/v1/query/start` accepts JSON with at least:

```json
{"sql":"SELECT owner, i, v FROM tenant_numbers WHERE owner = 'alice' ORDER BY i"}
```

with bearer auth such as:

```text
Authorization: Bearer demo-alice-token
```

It returns the first Arrow batch as the HTTP body when rows are available. If more fetches may be needed, it also returns:

- `X-Ducknng-Next-Token`
- `X-Ducknng-End-Of-Stream: false`
- `X-Ducknng-Tenant: <tenant_id>`
- `X-Ducknng-Subscriber: <subscriber_id>`

`/v1/query/fetch` accepts `{"token":"..."}` and either returns another Arrow batch with a replacement continuation token or returns `204` with `X-Ducknng-End-Of-Stream: true` after the backend session is exhausted.

`/v1/query/close` accepts `{"token":"..."}` and explicitly closes a live backend session when the client stops early. A valid token presented by the wrong tenant returns `403`.

## What this demonstrates

- auth-to-tenant and tenant-to-subscriber resolution from ordinary SQL tables
- multi-batch fetch continuation
- explicit early close
- cross-tenant close rejection
- Arrow IPC result transport over HTTP
- in-process multi-service topology with `service_serialized_connection`

What it does not try to do is reimplement DuckDB catalog or storage extension work. The goal is the low-level gateway and worker pattern on top of `ducknng`'s HTTP and session primitives.
