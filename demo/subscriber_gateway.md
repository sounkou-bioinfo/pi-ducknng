
<!-- demo/subscriber_gateway.md is generated from demo/subscriber_gateway.Rmd. Please edit that file -->

# Subscriber gateway demo

This is a rendered walkthrough of the subscriber gateway topology in
`ducknng`. All three services run inside a single in-process DuckDB
connection — no separate worker processes required — because each
service uses `service_serialized_connection`, giving it a dedicated
execution lane.

Render it from the repo root with:

``` sh
make subscriber_gateway_rdm
```

<details>
<summary>
<strong>Gateway Setup</strong>
</summary>

``` r
knitr::opts_chunk$set(collapse = TRUE, comment = "#>")

library(DBI)
library(duckdb)
library(jsonlite)
library(knitr)
library(nanonext)

project_root <- normalizePath("..", mustWork = TRUE)
extension_path <- normalizePath(
  file.path(project_root, "build", "release", "ducknng.duckdb_extension"),
  mustWork = FALSE
)

if (!file.exists(extension_path)) {
  stop("build/release/ducknng.duckdb_extension not found; run `make release` first.")
}

sql_quote <- function(text) gsub("'", "''", text, fixed = TRUE)

body_raw <- function(x) {
  if (is.null(x) || !length(x)) return(raw())
  if (typeof(x) == "raw") return(x)
  if (is.integer(x) || is.numeric(x)) return(as.raw(as.integer(x) %% 256L))
  if (is.character(x)) return(charToRaw(x))
  stop("unsupported response body type")
}

raw_hex <- function(x) paste(sprintf("%02X", as.integer(body_raw(x))), collapse = "")

header_value <- function(headers, name) {
  if (is.null(headers) || is.null(names(headers))) return(NA_character_)
  idx <- which(tolower(names(headers)) == tolower(name))
  if (!length(idx)) return(NA_character_)
  as.character(headers[[idx[[1L]]]])
}

response_summary <- function(resp) {
  data.frame(
    status        = resp$status,
    tenant        = header_value(resp$headers, "X-Ducknng-Tenant"),
    subscriber    = header_value(resp$headers, "X-Ducknng-Subscriber"),
    end_of_stream = header_value(resp$headers, "X-Ducknng-End-Of-Stream"),
    has_next_token = !is.na(header_value(resp$headers, "X-Ducknng-Next-Token")),
    stringsAsFactors = FALSE
  )
}

gateway_post <- function(base_url, path, payload, api_token = NULL) {
  headers <- c("Content-Type" = "application/json")
  if (!is.null(api_token)) {
    headers <- c(headers, "Authorization" = paste("Bearer", api_token))
  }
  ncurl(
    paste0(base_url, path),
    convert  = FALSE,
    response = TRUE,
    method   = "POST",
    headers  = headers,
    data     = charToRaw(toJSON(payload, auto_unbox = TRUE)),
    timeout  = 5000L
  )
}

wait_healthz <- function(base_url, timeout_s = 10) {
  deadline <- Sys.time() + timeout_s
  while (Sys.time() < deadline) {
    probe <- tryCatch(
      ncurl(paste0(base_url, "/healthz"), timeout = 500L),
      error = function(e) NULL
    )
    if (!is.null(probe) && identical(probe$status, 200L)) return(invisible(TRUE))
    Sys.sleep(0.1)
  }
  stop("gateway health check did not become ready")
}

# ---------------------------------------------------------------------------
# Route SQL
# ---------------------------------------------------------------------------
start_sql <- "
WITH req AS (
  SELECT
    r.headers_json,
    r.caller_identity,
    json_extract_string(TRY_CAST(b.body_text AS JSON), '$.sql') AS sql,
    coalesce(TRY_CAST(json_extract_string(TRY_CAST(b.body_text AS JSON), '$.batch_rows') AS UBIGINT), 0::UBIGINT) AS batch_rows,
    coalesce(TRY_CAST(json_extract_string(TRY_CAST(b.body_text AS JSON), '$.batch_bytes') AS UBIGINT), 0::UBIGINT) AS batch_bytes
  FROM ducknng_http_request() AS r, ducknng_http_request_body() AS b
),
header_auth AS (
  SELECT
    max(
      CASE
        WHEN lower(json_extract_string(value, '$.name')) = 'authorization'
          THEN NULLIF(regexp_extract(json_extract_string(value, '$.value'), '^Bearer[ ]+(.+)$', 1), '')
        ELSE NULL
      END
    ) AS bearer_token
  FROM req, json_each(coalesce(req.headers_json, '[]')::JSON)
),
auth AS (
  SELECT req.caller_identity, header_auth.bearer_token
  FROM req, header_auth
),
principal AS (
  SELECT
    i.tenant_id,
    i.principal_id,
    CASE
      WHEN auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity THEN 'caller_identity'
      ELSE 'bearer'
    END AS auth_source
  FROM auth
  JOIN gateway_identities AS i
    ON i.active AND (
      (auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity) OR
      (auth.bearer_token IS NOT NULL AND i.api_token = auth.bearer_token)
    )
  ORDER BY CASE
    WHEN auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity THEN 0
    ELSE 1
  END
  LIMIT 1
),
subscriber AS (
  SELECT s.subscriber_id, s.backend_url
  FROM principal
  JOIN gateway_subscribers AS s
    ON s.enabled AND s.tenant_id = principal.tenant_id
  ORDER BY s.priority, s.subscriber_id
  LIMIT 1
),
opened AS MATERIALIZED (
  SELECT
    principal.tenant_id,
    principal.principal_id,
    principal.auth_source,
    subscriber.subscriber_id,
    subscriber.backend_url,
    ducknng_open_query_raw(
      subscriber.backend_url,
      req.sql,
      req.batch_rows,
      req.batch_bytes,
      0::UBIGINT
    ) AS frame
  FROM req, principal, subscriber
  WHERE req.sql IS NOT NULL
),
open_decoded AS MATERIALIZED (
  SELECT
    opened.tenant_id,
    opened.principal_id,
    opened.auth_source,
    opened.subscriber_id,
    opened.backend_url,
    TRY_CAST(ducknng_frame_payload_text(opened.frame) AS JSON) AS payload_json,
    CASE
      WHEN ducknng_frame_error_text(opened.frame) IS NOT NULL
        THEN ducknng_frame_error_text(opened.frame)
      WHEN TRY_CAST(ducknng_frame_payload_text(opened.frame) AS JSON) IS NULL
        THEN 'ducknng: query_open reply payload was not valid JSON'
      WHEN json_extract(TRY_CAST(ducknng_frame_payload_text(opened.frame) AS JSON), '$.session_id')::UBIGINT IS NULL
        OR json_extract_string(TRY_CAST(ducknng_frame_payload_text(opened.frame) AS JSON), '$.session_token') IS NULL
        THEN 'ducknng: query_open reply did not include session_id and session_token'
      ELSE NULL
    END AS open_error
  FROM opened
),
continuation_seed AS MATERIALIZED (
  SELECT
    tenant_id,
    principal_id,
    auth_source,
    subscriber_id,
    backend_url,
    json_extract(payload_json, '$.session_id')::UBIGINT AS session_id,
    json_extract_string(payload_json, '$.session_token') AS session_token,
    (SELECT batch_rows FROM req LIMIT 1) AS batch_rows,
    (SELECT batch_bytes FROM req LIMIT 1) AS batch_bytes
  FROM open_decoded
  WHERE open_error IS NULL
),
fetched AS MATERIALIZED (
  SELECT
    continuation_seed.tenant_id,
    continuation_seed.principal_id,
    continuation_seed.auth_source,
    continuation_seed.subscriber_id,
    continuation_seed.backend_url,
    continuation_seed.session_id,
    continuation_seed.session_token,
    ducknng_fetch_query_raw(
      continuation_seed.backend_url,
      continuation_seed.session_id,
      continuation_seed.session_token,
      continuation_seed.batch_rows,
      continuation_seed.batch_bytes,
      0::UBIGINT
    ) AS frame
  FROM continuation_seed
),
fetch_decoded AS MATERIALIZED (
  SELECT
    fetched.tenant_id,
    fetched.principal_id,
    fetched.auth_source,
    fetched.subscriber_id,
    fetched.backend_url,
    fetched.session_id,
    fetched.session_token,
    ducknng_frame_error_text(fetched.frame) AS fetch_error,
    TRY_CAST(ducknng_frame_payload_text(fetched.frame) AS JSON) AS payload_json,
    ducknng_frame_payload(fetched.frame) AS payload,
    ducknng_frame_end_of_stream(fetched.frame) AS end_of_stream
  FROM fetched
),
continuation AS MATERIALIZED (
  SELECT
    tenant_id,
    principal_id,
    auth_source,
    subscriber_id,
    CAST(
      to_hex(
        encode(
          CAST(
            to_json(
              struct_pack(
                tenant_id    := tenant_id,
                principal_id := principal_id,
                auth_source  := auth_source,
                subscriber_id := subscriber_id,
                session_id   := session_id,
                session_token := session_token,
                backend_url  := backend_url
              )
            ) AS VARCHAR
          )
        )
      ) AS VARCHAR
    ) AS continuation_token,
    payload,
    fetch_error,
    coalesce(json_extract_string(payload_json, '$.state'), '') AS payload_state,
    end_of_stream
  FROM fetch_decoded
),
reply AS (
  SELECT
    CASE
      WHEN NOT EXISTS (SELECT 1 FROM principal)                                       THEN 401
      WHEN EXISTS (SELECT 1 FROM principal) AND NOT EXISTS (SELECT 1 FROM subscriber) THEN 503
      WHEN EXISTS (SELECT 1 FROM req WHERE sql IS NULL)                               THEN 400
      WHEN EXISTS (SELECT 1 FROM open_decoded WHERE open_error IS NOT NULL)           THEN 502
      WHEN EXISTS (SELECT 1 FROM continuation WHERE fetch_error IS NOT NULL)          THEN 502
      WHEN EXISTS (SELECT 1 FROM continuation WHERE fetch_error IS NULL AND payload_state = 'exhausted') THEN 204
      ELSE 200
    END AS status,
    CAST(
      to_json(
        list_filter(
          [
            struct_pack(name := 'X-Ducknng-Tenant',
              value := (SELECT tenant_id FROM principal LIMIT 1)),
            struct_pack(name := 'X-Ducknng-Subscriber',
              value := (SELECT subscriber_id FROM subscriber LIMIT 1)),
            struct_pack(
              name  := 'X-Ducknng-End-Of-Stream',
              value := CASE
                WHEN EXISTS (SELECT 1 FROM continuation WHERE fetch_error IS NULL AND (payload_state = 'exhausted' OR end_of_stream)) THEN 'true'
                WHEN EXISTS (SELECT 1 FROM continuation WHERE fetch_error IS NULL AND payload_state = '') THEN 'false'
                ELSE NULL
              END
            ),
            struct_pack(
              name  := 'X-Ducknng-Next-Token',
              value := CASE
                WHEN EXISTS (SELECT 1 FROM continuation WHERE fetch_error IS NULL AND payload_state <> 'exhausted' AND NOT end_of_stream)
                  THEN (SELECT continuation_token FROM continuation LIMIT 1)
                ELSE NULL
              END
            )
          ],
          x -> x.value IS NOT NULL
        )
      ) AS VARCHAR
    ) AS headers_json,
    'application/vnd.apache.arrow.stream' AS content_type,
    CAST(CASE
      WHEN EXISTS (SELECT 1 FROM continuation WHERE fetch_error IS NULL AND payload_state = '') THEN
        (SELECT payload FROM continuation WHERE fetch_error IS NULL AND payload_state = '' LIMIT 1)
      ELSE NULL
    END AS BLOB) AS body,
    CASE
      WHEN NOT EXISTS (SELECT 1 FROM principal)
        THEN CAST(to_json(struct_pack(error := 'missing or invalid bearer token')) AS VARCHAR)
      WHEN EXISTS (SELECT 1 FROM principal) AND NOT EXISTS (SELECT 1 FROM subscriber)
        THEN CAST(to_json(struct_pack(error := 'no subscriber available for tenant')) AS VARCHAR)
      WHEN EXISTS (SELECT 1 FROM req WHERE sql IS NULL)
        THEN CAST(to_json(struct_pack(error := 'request body must contain sql')) AS VARCHAR)
      WHEN EXISTS (SELECT 1 FROM open_decoded WHERE open_error IS NOT NULL)
        THEN CAST(to_json(struct_pack(error := (SELECT open_error FROM open_decoded WHERE open_error IS NOT NULL LIMIT 1))) AS VARCHAR)
      WHEN EXISTS (SELECT 1 FROM continuation WHERE fetch_error IS NOT NULL)
        THEN CAST(to_json(struct_pack(error := (SELECT fetch_error FROM continuation WHERE fetch_error IS NOT NULL LIMIT 1))) AS VARCHAR)
      ELSE NULL
    END AS body_text
  )
SELECT status, headers_json, content_type, body, body_text
FROM reply
"

fetch_sql <- "
WITH req AS (
  SELECT
    r.headers_json,
    r.caller_identity,
    json_extract_string(TRY_CAST(b.body_text AS JSON), '$.token') AS token
  FROM ducknng_http_request() AS r, ducknng_http_request_body() AS b
),
header_auth AS (
  SELECT
    max(
      CASE
        WHEN lower(json_extract_string(value, '$.name')) = 'authorization'
          THEN NULLIF(regexp_extract(json_extract_string(value, '$.value'), '^Bearer[ ]+(.+)$', 1), '')
        ELSE NULL
      END
    ) AS bearer_token
  FROM req, json_each(coalesce(req.headers_json, '[]')::JSON)
),
auth AS (
  SELECT req.caller_identity, header_auth.bearer_token
  FROM req, header_auth
),
principal AS (
  SELECT
    i.tenant_id,
    i.principal_id,
    CASE
      WHEN auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity THEN 'caller_identity'
      ELSE 'bearer'
    END AS auth_source
  FROM auth
  JOIN gateway_identities AS i
    ON i.active AND (
      (auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity) OR
      (auth.bearer_token IS NOT NULL AND i.api_token = auth.bearer_token)
    )
  ORDER BY CASE
    WHEN auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity THEN 0
    ELSE 1
  END
  LIMIT 1
),
token_raw AS MATERIALIZED (
  SELECT TRY_CAST(decode(from_hex(token)) AS VARCHAR) AS token_json
  FROM req
  WHERE token IS NOT NULL
),
token_meta AS MATERIALIZED (
  SELECT
    json_extract_string(token_json::JSON, '$.tenant_id')    AS tenant_id,
    json_extract_string(token_json::JSON, '$.principal_id') AS principal_id,
    json_extract_string(token_json::JSON, '$.auth_source')  AS auth_source,
    json_extract_string(token_json::JSON, '$.subscriber_id') AS subscriber_id,
    json_extract(token_json::JSON, '$.session_id')::UBIGINT  AS session_id,
    json_extract_string(token_json::JSON, '$.session_token') AS session_token,
    json_extract_string(token_json::JSON, '$.backend_url')   AS backend_url
  FROM token_raw
  WHERE token_json IS NOT NULL
),
token_ready AS MATERIALIZED (
  SELECT * FROM token_meta
  WHERE tenant_id IS NOT NULL AND principal_id IS NOT NULL
    AND auth_source IS NOT NULL AND subscriber_id IS NOT NULL
    AND session_id IS NOT NULL AND session_token IS NOT NULL
    AND backend_url IS NOT NULL
),
authorized AS MATERIALIZED (
  SELECT * FROM principal, token_ready
  WHERE principal.tenant_id   = token_ready.tenant_id
    AND principal.principal_id = token_ready.principal_id
    AND principal.auth_source  = token_ready.auth_source
),
fetched AS MATERIALIZED (
  SELECT
    authorized.tenant_id,
    authorized.subscriber_id,
    ducknng_fetch_query_raw(
      authorized.backend_url,
      authorized.session_id,
      authorized.session_token,
      0::UBIGINT,
      0::UBIGINT,
      0::UBIGINT
    ) AS frame
  FROM authorized
),
decoded AS MATERIALIZED (
  SELECT
    tenant_id,
    subscriber_id,
    ducknng_frame_error_text(frame) AS frame_error,
    TRY_CAST(ducknng_frame_payload_text(fetched.frame) AS JSON) AS payload_json,
    ducknng_frame_payload(fetched.frame) AS payload,
    ducknng_frame_end_of_stream(fetched.frame) AS end_of_stream
  FROM fetched
),
continuation AS MATERIALIZED (
  SELECT
    tenant_id,
    subscriber_id,
    CAST(
      to_hex(
        encode(
          CAST(
            to_json(
              struct_pack(
                tenant_id    := (SELECT tenant_id    FROM authorized LIMIT 1),
                principal_id := (SELECT principal_id FROM authorized LIMIT 1),
                auth_source  := (SELECT auth_source  FROM authorized LIMIT 1),
                subscriber_id := subscriber_id,
                session_id   := (SELECT session_id   FROM authorized LIMIT 1),
                session_token := (SELECT session_token FROM authorized LIMIT 1),
                backend_url  := (SELECT backend_url  FROM authorized LIMIT 1)
              )
            ) AS VARCHAR
          )
        )
      ) AS VARCHAR
    ) AS continuation_token,
    payload,
    frame_error,
    coalesce(json_extract_string(payload_json, '$.state'), '') AS payload_state,
    end_of_stream
  FROM decoded
),
reply AS (
  SELECT
    CASE
      WHEN NOT EXISTS (SELECT 1 FROM principal)                                          THEN 401
      WHEN NOT EXISTS (SELECT 1 FROM token_ready)                                        THEN 400
      WHEN NOT EXISTS (SELECT 1 FROM authorized)                                         THEN 403
      WHEN EXISTS (SELECT 1 FROM continuation WHERE frame_error IS NOT NULL)             THEN 502
      WHEN EXISTS (SELECT 1 FROM continuation WHERE payload_state = 'exhausted')         THEN 204
      ELSE 200
    END AS status,
    CAST(
      to_json(
        list_filter(
          [
            struct_pack(name := 'X-Ducknng-Tenant',
              value := (SELECT tenant_id FROM authorized LIMIT 1)),
            struct_pack(name := 'X-Ducknng-Subscriber',
              value := (SELECT subscriber_id FROM authorized LIMIT 1)),
            struct_pack(
              name  := 'X-Ducknng-End-Of-Stream',
              value := CASE
                WHEN EXISTS (SELECT 1 FROM continuation WHERE frame_error IS NULL AND (payload_state = 'exhausted' OR end_of_stream)) THEN 'true'
                WHEN EXISTS (SELECT 1 FROM continuation WHERE frame_error IS NULL AND payload_state = '') THEN 'false'
                ELSE NULL
              END
            ),
            struct_pack(
              name  := 'X-Ducknng-Next-Token',
              value := CASE
                WHEN EXISTS (SELECT 1 FROM continuation WHERE frame_error IS NULL AND payload_state <> 'exhausted' AND NOT end_of_stream)
                  THEN (SELECT continuation_token FROM continuation LIMIT 1)
                ELSE NULL
              END
            )
          ],
          x -> x.value IS NOT NULL
        )
      ) AS VARCHAR
    ) AS headers_json,
    'application/vnd.apache.arrow.stream' AS content_type,
    CAST(CASE
      WHEN EXISTS (SELECT 1 FROM continuation WHERE frame_error IS NULL AND payload_state = '') THEN
        (SELECT payload FROM continuation WHERE frame_error IS NULL LIMIT 1)
      ELSE NULL
    END AS BLOB) AS body,
    CASE
      WHEN NOT EXISTS (SELECT 1 FROM principal)
        THEN CAST(to_json(struct_pack(error := 'missing or invalid bearer token')) AS VARCHAR)
      WHEN NOT EXISTS (SELECT 1 FROM token_ready)
        THEN CAST(to_json(struct_pack(error := 'invalid continuation token')) AS VARCHAR)
      WHEN NOT EXISTS (SELECT 1 FROM authorized)
        THEN CAST(to_json(struct_pack(error := 'continuation token belongs to another tenant or principal')) AS VARCHAR)
      WHEN EXISTS (SELECT 1 FROM continuation WHERE frame_error IS NOT NULL)
        THEN CAST(to_json(struct_pack(error := (SELECT frame_error FROM continuation WHERE frame_error IS NOT NULL LIMIT 1))) AS VARCHAR)
      ELSE NULL
    END AS body_text
  )
SELECT status, headers_json, content_type, body, body_text
FROM reply
"

close_sql <- "
DROP TABLE IF EXISTS __ducknng_gateway_close_state;
DROP TABLE IF EXISTS __ducknng_gateway_close_meta;
DROP TABLE IF EXISTS __ducknng_gateway_close_reply;
CREATE TEMP TABLE __ducknng_gateway_close_state AS
WITH req AS (
  SELECT
    r.headers_json,
    r.caller_identity,
    json_extract_string(TRY_CAST(b.body_text AS JSON), '$.token') AS token
  FROM ducknng_http_request() AS r, ducknng_http_request_body() AS b
),
header_auth AS (
  SELECT
    max(
      CASE
        WHEN lower(json_extract_string(value, '$.name')) = 'authorization'
          THEN NULLIF(regexp_extract(json_extract_string(value, '$.value'), '^Bearer[ ]+(.+)$', 1), '')
        ELSE NULL
      END
    ) AS bearer_token
  FROM req, json_each(coalesce(req.headers_json, '[]')::JSON)
),
auth AS (SELECT req.caller_identity, header_auth.bearer_token FROM req, header_auth),
principal AS MATERIALIZED (
  SELECT
    i.tenant_id,
    i.principal_id,
    CASE
      WHEN auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity THEN 'caller_identity'
      ELSE 'bearer'
    END AS auth_source
  FROM auth
  JOIN gateway_identities AS i
    ON i.active AND (
      (auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity) OR
      (auth.bearer_token IS NOT NULL AND i.api_token = auth.bearer_token)
    )
  ORDER BY CASE
    WHEN auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity THEN 0
    ELSE 1
  END
  LIMIT 1
),
token_raw AS MATERIALIZED (
  SELECT TRY_CAST(decode(from_hex(token)) AS VARCHAR) AS token_json
  FROM req WHERE token IS NOT NULL
),
token_meta AS MATERIALIZED (
  SELECT
    json_extract_string(token_json::JSON, '$.tenant_id')    AS tenant_id,
    json_extract_string(token_json::JSON, '$.principal_id') AS principal_id,
    json_extract_string(token_json::JSON, '$.auth_source')  AS auth_source,
    json_extract_string(token_json::JSON, '$.subscriber_id') AS subscriber_id,
    json_extract(token_json::JSON, '$.session_id')::UBIGINT  AS session_id,
    json_extract_string(token_json::JSON, '$.session_token') AS session_token,
    json_extract_string(token_json::JSON, '$.backend_url')   AS backend_url
  FROM token_raw WHERE token_json IS NOT NULL
),
token_ready AS MATERIALIZED (
  SELECT * FROM token_meta
  WHERE tenant_id IS NOT NULL AND principal_id IS NOT NULL
    AND auth_source IS NOT NULL AND subscriber_id IS NOT NULL
    AND session_id IS NOT NULL AND session_token IS NOT NULL
    AND backend_url IS NOT NULL
),
authorized AS MATERIALIZED (
  SELECT * FROM principal, token_ready
  WHERE principal.tenant_id   = token_ready.tenant_id
    AND principal.principal_id = token_ready.principal_id
    AND principal.auth_source  = token_ready.auth_source
)
SELECT
  EXISTS (SELECT 1 FROM principal)    AS has_principal,
  EXISTS (SELECT 1 FROM token_ready)  AS has_token_meta,
  EXISTS (SELECT 1 FROM authorized)   AS is_authorized,
  (SELECT tenant_id    FROM authorized LIMIT 1) AS tenant_id,
  (SELECT subscriber_id FROM authorized LIMIT 1) AS subscriber_id,
  (SELECT backend_url  FROM authorized LIMIT 1) AS backend_url,
  (SELECT session_id   FROM authorized LIMIT 1) AS session_id,
  (SELECT session_token FROM authorized LIMIT 1) AS session_token;
CREATE TEMP TABLE __ducknng_gateway_close_meta AS
SELECT
  tenant_id,
  subscriber_id,
  ducknng_frame_error_text(
    ducknng_close_query_raw(backend_url, session_id, session_token, 0::UBIGINT)
  ) AS close_error
FROM __ducknng_gateway_close_state
WHERE is_authorized;
CREATE TEMP TABLE __ducknng_gateway_close_reply AS
SELECT
  CASE
    WHEN NOT has_principal  THEN 401
    WHEN NOT has_token_meta THEN 400
    WHEN NOT is_authorized  THEN 403
    WHEN EXISTS (SELECT 1 FROM __ducknng_gateway_close_meta WHERE close_error IS NOT NULL) THEN 502
    ELSE 200
  END AS status,
  'application/json; charset=utf-8' AS content_type,
  CASE
    WHEN NOT has_principal
      THEN CAST(to_json(struct_pack(closed := FALSE, error := 'missing or invalid bearer token')) AS VARCHAR)
    WHEN NOT has_token_meta
      THEN CAST(to_json(struct_pack(closed := FALSE, error := 'invalid continuation token')) AS VARCHAR)
    WHEN NOT is_authorized
      THEN CAST(to_json(struct_pack(closed := FALSE, error := 'continuation token belongs to another tenant or principal')) AS VARCHAR)
    WHEN EXISTS (SELECT 1 FROM __ducknng_gateway_close_meta WHERE close_error IS NOT NULL)
      THEN CAST(to_json(struct_pack(closed := FALSE,
             error := (SELECT close_error FROM __ducknng_gateway_close_meta
                       WHERE close_error IS NOT NULL LIMIT 1))) AS VARCHAR)
    ELSE CAST(to_json(struct_pack(
           closed      := TRUE,
           tenant_id   := (SELECT tenant_id    FROM __ducknng_gateway_close_meta LIMIT 1),
           subscriber_id := (SELECT subscriber_id FROM __ducknng_gateway_close_meta LIMIT 1)
         )) AS VARCHAR)
  END AS body_text
FROM __ducknng_gateway_close_state;
SELECT status, content_type, body_text
FROM __ducknng_gateway_close_reply
"

# ---------------------------------------------------------------------------
# Start all services in a single in-process DuckDB connection
# ---------------------------------------------------------------------------
con <- dbConnect(duckdb(config = list(allow_unsigned_extensions = "true", allow_extensions_metadata_mismatch = "true")))
dbExecute(con, sprintf("LOAD '%s'", sql_quote(extension_path)))
dbExecute(con, "INSTALL json")
dbExecute(con, "LOAD json")

# Shared tenant data: alice rows i=1..3000, bob rows i=10001..13000.
# Both services share this table; client SQL uses WHERE owner = '...' to
# read only their own rows. In a production deployment each subscriber
# backend would be a separate DuckDB database.
dbExecute(con, "CREATE TABLE tenant_numbers(owner VARCHAR, i INTEGER, v INTEGER)")
dbExecute(con,
  "INSERT INTO tenant_numbers
   SELECT 'alice' AS owner, i, i * 10 AS v FROM range(1, 3001) AS t(i)"
)
dbExecute(con,
  "INSERT INTO tenant_numbers
   SELECT 'bob' AS owner, i, i * 10 AS v FROM range(10001, 13001) AS t(i)"
)

# Backend: subscriber_alice
dbExecute(con,
  "SELECT ducknng_start_server('subscriber_alice',
     'tcp://127.0.0.1:0', 1, 134217728, 300000, 0::UBIGINT)"
)
dbExecute(con,
  "SELECT ducknng_set_service_execution_model(
     'subscriber_alice', 'service_serialized_connection')"
)
alice_backend_url <- dbGetQuery(con,
  "SELECT listen FROM ducknng_list_servers() WHERE name = 'subscriber_alice'"
)$listen

# Backend: subscriber_bob
dbExecute(con,
  "SELECT ducknng_start_server('subscriber_bob',
     'tcp://127.0.0.1:0', 1, 134217728, 300000, 0::UBIGINT)"
)
dbExecute(con,
  "SELECT ducknng_set_service_execution_model(
     'subscriber_bob', 'service_serialized_connection')"
)
bob_backend_url <- dbGetQuery(con,
  "SELECT listen FROM ducknng_list_servers() WHERE name = 'subscriber_bob'"
)$listen

# Gateway tables
dbExecute(con,
  "CREATE TABLE gateway_identities(
     api_token VARCHAR, caller_identity VARCHAR,
     tenant_id VARCHAR, principal_id VARCHAR, active BOOLEAN)"
)
dbExecute(con,
  "INSERT INTO gateway_identities VALUES
     ('demo-alice-token', NULL, 'tenant_alice', 'demo:alice', TRUE),
     ('demo-bob-token',   NULL, 'tenant_bob',   'demo:bob',   TRUE),
     ('demo-orphan-token', NULL, 'tenant_orphan', 'demo:orphan', TRUE)"
)
dbExecute(con,
  "CREATE TABLE gateway_subscribers(
     subscriber_id VARCHAR, tenant_id VARCHAR,
     backend_url VARCHAR, priority INTEGER, enabled BOOLEAN)"
)
dbExecute(con, sprintf(
  "INSERT INTO gateway_subscribers VALUES
     ('alice_worker', 'tenant_alice', '%s', 1, TRUE),
     ('bob_worker',   'tenant_bob',   '%s', 1, TRUE)",
  sql_quote(alice_backend_url),
  sql_quote(bob_backend_url)
))

# Gateway HTTP service
dbExecute(con,
  "SELECT ducknng_start_server('gateway',
     'http://127.0.0.1:0/_ducknng', 1, 134217728, 300000, 0::UBIGINT)"
)
dbExecute(con,
  "SELECT ducknng_set_service_execution_model(
     'gateway', 'service_serialized_connection')"
)
gateway_listen <- dbGetQuery(con,
  "SELECT replace(listen, '/_ducknng', '') FROM ducknng_list_servers()
   WHERE name = 'gateway'"
)[[1]]
gateway_base_url <- gateway_listen

# Routes
dbExecute(con,
  "SELECT ducknng_register_http_route('gateway', 'GET', '/healthz',
     'SELECT 200 AS status,
             ''text/plain; charset=utf-8'' AS content_type,
             ''ok'' AS body_text')"
)
dbExecute(con, sprintf(
  "SELECT ducknng_register_http_route('gateway', 'POST', '/v1/query/start',
     '%s', 1048576::UBIGINT)",
  sql_quote(start_sql)
))
dbExecute(con, sprintf(
  "SELECT ducknng_register_http_route('gateway', 'POST', '/v1/query/fetch',
     '%s', 1048576::UBIGINT)",
  sql_quote(fetch_sql)
))
dbExecute(con, sprintf(
  "SELECT ducknng_register_http_route('gateway', 'POST', '/v1/query/close',
     '%s', 1048576::UBIGINT)",
  sql_quote(close_sql)
))

alice_token <- "demo-alice-token"
bob_token   <- "demo-bob-token"

wait_healthz(gateway_base_url)
```

</details>

## Topology

One gateway service owns the public HTTP edge, identity resolution, and
subscriber lookup. Two private backend services expose ordinary
`ducknng` query-session services over TCP. All three share one in-memory
DuckDB database, and all three use `service_serialized_connection` so
they each have their own dedicated connection lane.

``` text
HTTP client
    |
    v
gateway service (service_serialized_connection)
  http://127.0.0.1:<auto>/_ducknng
    |
    +--> subscriber_alice  tcp://127.0.0.1:<auto>
    |    (service_serialized_connection)
    |
    +--> subscriber_bob    tcp://127.0.0.1:<auto>
         (service_serialized_connection)
```

Because gateway route SQL calls
`ducknng_open_query_raw(backend_url, ...)` synchronously, the gateway’s
connection blocks while waiting for the backend reply. Since each
backend listens on its own connection, it can respond independently — no
deadlock, even though all three services share one DuckDB runtime. In a
production deployment each subscriber backend would be a separate DuckDB
process with its own isolated database.

The route layer stays generic because the gateway owns two SQL tables:

``` sql
CREATE TABLE gateway_identities(
  api_token VARCHAR,
  caller_identity VARCHAR,
  tenant_id VARCHAR,
  principal_id VARCHAR,
  active BOOLEAN
);

CREATE TABLE gateway_subscribers(
  subscriber_id VARCHAR,
  tenant_id VARCHAR,
  backend_url VARCHAR,
  priority INTEGER,
  enabled BOOLEAN
);
```

`gateway_identities` resolves bearer tokens into a tenant and principal.
`gateway_subscribers` resolves that tenant to one private backend URL.
Nothing in the public route SQL needs to know whether the backend plane
has two workers or two hundred.

## Starting the services

All three services start in one DuckDB connection. Backend services
start first so their assigned TCP ports are available when building the
gateway’s subscriber table.

``` r
# Backend: subscriber_alice (OS assigns the port)
dbExecute(con,
  "SELECT ducknng_start_server('subscriber_alice',
     'tcp://127.0.0.1:0', 1, 134217728, 300000, 0::UBIGINT)"
)
dbExecute(con,
  "SELECT ducknng_set_service_execution_model(
     'subscriber_alice', 'service_serialized_connection')"
)

# Backend: subscriber_bob
dbExecute(con,
  "SELECT ducknng_start_server('subscriber_bob',
     'tcp://127.0.0.1:0', 1, 134217728, 300000, 0::UBIGINT)"
)
dbExecute(con,
  "SELECT ducknng_set_service_execution_model(
     'subscriber_bob', 'service_serialized_connection')"
)

# Gateway HTTP service
dbExecute(con,
  "SELECT ducknng_start_server('gateway',
     'http://127.0.0.1:0/_ducknng', 1, 134217728, 300000, 0::UBIGINT)"
)
dbExecute(con,
  "SELECT ducknng_set_service_execution_model(
     'gateway', 'service_serialized_connection')"
)
```

## Gateway route shape

The public edge is three HTTP routes, all registered with a 1 MiB body
limit:

``` sql
SELECT ducknng_register_http_route('gateway', 'GET',  '/healthz',         ...);
SELECT ducknng_register_http_route('gateway', 'POST', '/v1/query/start',  ..., 1048576::UBIGINT);
SELECT ducknng_register_http_route('gateway', 'POST', '/v1/query/fetch',  ..., 1048576::UBIGINT);
SELECT ducknng_register_http_route('gateway', 'POST', '/v1/query/close',  ..., 1048576::UBIGINT);
```

Inside those routes the gateway uses the raw session helpers so
request-body columns and continuation-token columns stay dynamic:

``` sql
ducknng_open_query_raw(...)
ducknng_fetch_query_raw(...)
ducknng_close_query_raw(...)
ducknng_frame_payload(...)
ducknng_frame_payload_text(...)
ducknng_frame_error_text(...)
```

That keeps the route layer transport-local and gateway-owned. The public
client sees HTTP plus Arrow batches; the private worker plane stays on
the ordinary `ducknng` session contract.

## Alice starts a query through the public gateway

``` r
start <- ncurl(
  paste0(gateway_base_url, "/v1/query/start"),
  convert  = FALSE,
  response = TRUE,
  method   = "POST",
  headers  = c(
    "Content-Type"  = "application/json",
    "Authorization" = "Bearer demo-alice-token"
  ),
  data    = charToRaw(
    '{"sql":"SELECT owner, i, v FROM tenant_numbers WHERE owner = \'alice\' ORDER BY i",
      "batch_rows":4}'
  ),
  timeout = 5000L
)
```

| status | tenant       | subscriber   | end_of_stream | has_next_token |
|-------:|:-------------|:-------------|:--------------|:---------------|
|    200 | tenant_alice | alice_worker | false         | TRUE           |

The response body is an Arrow IPC batch. The gateway resolved the bearer
token, chose `alice_worker` as the backend, and embedded backend session
state in the opaque `X-Ducknng-Next-Token`.

| owner |   i |   v |
|:------|----:|----:|
| alice |   1 |  10 |
| alice |   2 |  20 |
| alice |   3 |  30 |
| alice |   4 |  40 |

## Fetch continues on the same private subscriber

``` r
fetch <- ncurl(
  paste0(gateway_base_url, "/v1/query/fetch"),
  convert  = FALSE,
  response = TRUE,
  method   = "POST",
  headers  = c(
    "Content-Type"  = "application/json",
    "Authorization" = "Bearer demo-alice-token"
  ),
  data    = charToRaw(sprintf('{"token":"%s"}', alice_next_token)),
  timeout = 5000L
)
```

| status | tenant       | subscriber   | end_of_stream | has_next_token |
|-------:|:-------------|:-------------|:--------------|:---------------|
|    200 | tenant_alice | alice_worker | false         | TRUE           |

| owner |    i |     v |
|:------|-----:|------:|
| alice | 2049 | 20490 |
| alice | 2050 | 20500 |
| alice | 2051 | 20510 |
| alice | 2052 | 20520 |

The public continuation token is gateway-owned. It carries tenant and
subscriber affinity plus the private backend `session_id` and
`session_token` without exposing them directly to the client.

## A second identity routes to a different subscriber

``` r
bob_start <- ncurl(
  paste0(gateway_base_url, "/v1/query/start"),
  convert  = FALSE,
  response = TRUE,
  method   = "POST",
  headers  = c(
    "Content-Type"  = "application/json",
    "Authorization" = "Bearer demo-bob-token"
  ),
  data    = charToRaw(
    '{"sql":"SELECT owner, i, v FROM tenant_numbers WHERE owner = \'bob\' ORDER BY i",
      "batch_rows":4}'
  ),
  timeout = 5000L
)
```

| status | tenant     | subscriber | end_of_stream | has_next_token |
|-------:|:-----------|:-----------|:--------------|:---------------|
|    200 | tenant_bob | bob_worker | false         | TRUE           |

| owner |     i |      v |
|:------|------:|-------:|
| bob   | 10001 | 100010 |
| bob   | 10002 | 100020 |
| bob   | 10003 | 100030 |
| bob   | 10004 | 100040 |

The public start route did not change. Only the authenticated identity
changed, and the gateway routed the request to `bob_worker` rather than
`alice_worker`.

## Cross-tenant close is rejected

``` r
wrong_close <- ncurl(
  paste0(gateway_base_url, "/v1/query/close"),
  convert  = FALSE,
  response = TRUE,
  method   = "POST",
  headers  = c(
    "Content-Type"  = "application/json",
    "Authorization" = "Bearer demo-bob-token"
  ),
  data    = charToRaw(sprintf('{"token":"%s"}', alice_next_token)),
  timeout = 5000L
)
```

| status | body                                                                                 |
|-------:|:-------------------------------------------------------------------------------------|
|    403 | {“closed”:false,“error”:“continuation token belongs to another tenant or principal”} |

The `403` is the important ownership boundary: a gateway token is not a
cross-tenant escape hatch.

## The owning tenant can close the session

``` r
close <- ncurl(
  paste0(gateway_base_url, "/v1/query/close"),
  convert  = FALSE,
  response = TRUE,
  method   = "POST",
  headers  = c(
    "Content-Type"  = "application/json",
    "Authorization" = "Bearer demo-alice-token"
  ),
  data    = charToRaw(sprintf('{"token":"%s"}', alice_next_token)),
  timeout = 5000L
)
```

| status | body                                                                      |
|-------:|:--------------------------------------------------------------------------|
|    200 | {“closed”:true,“tenant_id”:“tenant_alice”,“subscriber_id”:“alice_worker”} |

## What this shape buys you

This topology is generic in the right place:

- the public route surface stays fixed
- auth resolution is table-driven
- subscriber discovery is table-driven
- backend session affinity stays private
- Arrow remains the row contract

The `service_serialized_connection` model makes all of this work within
a single DuckDB runtime: each service has its own connection lane, so a
gateway route handler can make synchronous NNG calls to sibling services
without deadlocking.
