ducknng bulk transport benchmark
================

- [Machine and software details](#machine-and-software-details)
- [RPC bulk transfer results](#rpc-bulk-transfer-results)
- [Concurrent reader/writer results](#concurrent-readerwriter-results)
- [HTTP ducknng vs Quack ratio](#http-ducknng-vs-quack-ratio)
- [Notes](#notes)

This report is rendered by `make rpc_bulk_compare`. The default workload
uses `DUCKNNG_BULK_ROWS=100000,1000000,10000000` and
`DUCKNNG_BULK_REPETITIONS=5`; set those environment variables when you
want a smoke render instead of the full benchmark. The concurrent slice
uses `DUCKNNG_CONCURRENT_ROWS`, `DUCKNNG_CONCURRENT_ITERATIONS`, and
`DUCKNNG_CONCURRENT_CLIENTS` with defaults derived from the row-transfer
workload.

It measures three things:

1.  `ducknng` bulk row transfer over its RPC/session surface on `http`,
    `tcp`, `ipc`, and `ws`, in both `arrow_ipc_stream` and
    `ducknng_quack_batch` modes.
2.  concurrent ducknng reader, writer, and mixed reader/writer RPC
    workloads across those transports and serializers.
3.  `quack` bulk row transfer over its published client/server surface.

`quack` is only compared on its own public client path. `ducknng` is
compared across multiple transports because transport selection is part
of its SQL-facing contract.

## Machine and software details

| Field                       | Value                                  |
|:----------------------------|:---------------------------------------|
| generated_at                | 2026-05-17 15:37:58 UTC                |
| hostname                    | Ubuntu-2404-noble-amd64-base           |
| sysname                     | Linux                                  |
| release                     | 6.8.0-78-generic                       |
| machine                     | x86_64                                 |
| cpu_model                   | 13th Gen Intel(R) Core(TM) i5-13500    |
| logical_cores               | 20                                     |
| physical_cores              | 20                                     |
| memory_total                | 62.6 GiB                               |
| r_version                   | R version 4.6.0 (2026-04-24)           |
| duckdb_version              | 1.5.2                                  |
| ducknng_extension           | build/release/ducknng.duckdb_extension |
| ducknng_git_commit          | 5729247                                |
| quack_install_source        | INSTALL quack FROM core_nightly        |
| dataset                     | tpch_sf2.lineitem                      |
| lineitem_rows_available     | 11997996                               |
| repetitions                 | 5                                      |
| ducknng_transports          | http,tcp,ipc,ws                        |
| ducknng_serialization_modes | arrow_ipc_stream,ducknng_quack_batch   |
| concurrent_rows             | 100000                                 |
| concurrent_iterations       | 2                                      |
| concurrent_clients          | 2                                      |
| quack_uri                   | quack:localhost:19494                  |

## RPC bulk transfer results

These runs execute `SELECT * FROM lineitem LIMIT n` against the server
side and validate the returned data by aggregate checksum.

| benchmark                    | dataset           | system  | protocol    | transport | serialization_mode  |     rows | repetitions | median_seconds | min_seconds | max_seconds | timings_seconds               |
|:-----------------------------|:------------------|:--------|:------------|:----------|:--------------------|---------:|------------:|---------------:|------------:|------------:|:------------------------------|
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | arrow_ipc_stream    |   100000 |           5 |          0.096 |       0.090 |       0.101 | 0.100,0.090,0.091,0.096,0.101 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | arrow_ipc_stream    |  1000000 |           5 |          0.812 |       0.783 |       0.964 | 0.812,0.783,0.847,0.964,0.804 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | arrow_ipc_stream    | 10000000 |           5 |          6.104 |       5.972 |       6.349 | 6.349,6.104,6.010,5.972,6.202 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | ducknng_quack_batch |   100000 |           5 |          0.066 |       0.058 |       0.067 | 0.066,0.061,0.058,0.067,0.067 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | ducknng_quack_batch |  1000000 |           5 |          0.645 |       0.588 |       0.676 | 0.607,0.676,0.647,0.645,0.588 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | ducknng_quack_batch | 10000000 |           5 |          3.742 |       3.692 |       3.931 | 3.756,3.732,3.742,3.692,3.931 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | quack   | quack_query | http      | application/vnd.duckdb  |   100000 |           5 |          0.086 |       0.081 |       0.097 | 0.097,0.085,0.081,0.094,0.086 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | quack   | quack_query | http      | application/vnd.duckdb  |  1000000 |           5 |          0.685 |       0.641 |       0.872 | 0.872,0.711,0.682,0.685,0.641 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | quack   | quack_query | http      | application/vnd.duckdb  | 10000000 |           5 |          4.591 |       4.400 |       4.739 | 4.423,4.400,4.591,4.674,4.739 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | arrow_ipc_stream    |   100000 |           5 |          0.077 |       0.076 |       0.088 | 0.080,0.088,0.076,0.076,0.077 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | arrow_ipc_stream    |  1000000 |           5 |          0.771 |       0.718 |       0.789 | 0.771,0.789,0.725,0.782,0.718 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | arrow_ipc_stream    | 10000000 |           5 |          5.539 |       5.171 |       5.868 | 5.723,5.868,5.539,5.171,5.375 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | ducknng_quack_batch |   100000 |           5 |          0.060 |       0.056 |       0.064 | 0.060,0.061,0.057,0.056,0.064 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | ducknng_quack_batch |  1000000 |           5 |          0.511 |       0.494 |       0.526 | 0.526,0.512,0.494,0.511,0.504 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | ducknng_quack_batch | 10000000 |           5 |          4.203 |       3.277 |       4.350 | 3.277,3.375,4.350,4.329,4.203 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | arrow_ipc_stream    |   100000 |           5 |          0.090 |       0.078 |       0.105 | 0.078,0.079,0.105,0.090,0.094 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | arrow_ipc_stream    |  1000000 |           5 |          0.689 |       0.659 |       0.718 | 0.689,0.659,0.666,0.707,0.718 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | arrow_ipc_stream    | 10000000 |           5 |          5.075 |       5.004 |       5.323 | 5.075,5.323,5.083,5.004,5.022 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | ducknng_quack_batch |   100000 |           5 |          0.055 |       0.054 |       0.057 | 0.054,0.057,0.055,0.054,0.057 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | ducknng_quack_batch |  1000000 |           5 |          0.489 |       0.482 |       0.556 | 0.506,0.483,0.482,0.489,0.556 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | ducknng_quack_batch | 10000000 |           5 |          3.400 |       3.174 |       4.066 | 3.400,3.174,3.394,3.544,4.066 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | arrow_ipc_stream    |   100000 |           5 |          0.088 |       0.082 |       0.089 | 0.085,0.089,0.089,0.088,0.082 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | arrow_ipc_stream    |  1000000 |           5 |          0.747 |       0.725 |       0.763 | 0.763,0.725,0.755,0.743,0.747 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | arrow_ipc_stream    | 10000000 |           5 |          5.936 |       5.793 |       6.447 | 5.793,6.056,5.867,5.936,6.447 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | ducknng_quack_batch |   100000 |           5 |          0.060 |       0.059 |       0.063 | 0.059,0.059,0.063,0.060,0.061 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | ducknng_quack_batch |  1000000 |           5 |          0.535 |       0.531 |       0.551 | 0.551,0.536,0.531,0.535,0.533 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | ducknng_quack_batch | 10000000 |           5 |          3.696 |       3.587 |       4.662 | 3.587,3.696,3.621,3.798,4.662 |

## Concurrent reader/writer results

These runs open multiple client connections against the same ducknng
service. Reader workers fetch row batches through
`ducknng_query_rpc(...)` / `ducknng_query_rpc_mode(...)`; writer workers
issue set-oriented `INSERT ... SELECT FROM range(...)` statements
through `ducknng_run_rpc(...)`. The mixed scenario runs the configured
number of readers and writers at the same time. `wall_seconds` includes
benchmark orchestration around the worker calls; throughput columns use
`operation_seconds`, the maximum measured in-worker operation time, so
writer startup and extension-load overhead do not dominate the
write-rate numbers.

| benchmark             | system  | protocol | transport | serialization_mode  | scenario         | readers | writers | rows_per_reader_operation | iterations_per_worker | wall_seconds | operation_seconds | rows_read | rows_written | write_ops | rows_per_second | write_rows_per_second | write_ops_per_second | median_latency_seconds | p95_latency_seconds |
|:----------------------|:--------|:---------|:----------|:--------------------|:-----------------|--------:|--------:|--------------------------:|----------------------:|-------------:|------------------:|----------:|-------------:|----------:|----------------:|----------------------:|---------------------:|-----------------------:|--------------------:|
| concurrent_read_write | ducknng | rpc      | http      | arrow_ipc_stream    | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.832 |             0.122 |    400000 |       400000 |         4 |         3278688 |               3278688 |                 32.8 |                  0.034 |               0.091 |
| concurrent_read_write | ducknng | rpc      | http      | arrow_ipc_stream    | readers          |       2 |       0 |                    100000 |                     2 |        0.676 |             0.075 |    400000 |            0 |         0 |         5333333 |                     0 |                  0.0 |                  0.037 |               0.042 |
| concurrent_read_write | ducknng | rpc      | http      | arrow_ipc_stream    | writers          |       0 |       2 |                    100000 |                     2 |        0.605 |             0.005 |         0 |       400000 |         4 |               0 |              80000000 |                800.0 |                  0.003 |               0.003 |
| concurrent_read_write | ducknng | rpc      | http      | ducknng_quack_batch | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.820 |             0.089 |    400000 |       400000 |         4 |         4494382 |               4494382 |                 44.9 |                  0.026 |               0.056 |
| concurrent_read_write | ducknng | rpc      | http      | ducknng_quack_batch | readers          |       2 |       0 |                    100000 |                     2 |        0.804 |             0.090 |    400000 |            0 |         0 |         4444444 |                     0 |                  0.0 |                  0.041 |               0.058 |
| concurrent_read_write | ducknng | rpc      | http      | ducknng_quack_batch | writers          |       0 |       2 |                    100000 |                     2 |        0.655 |             0.008 |         0 |       400000 |         4 |               0 |              50000000 |                500.0 |                  0.004 |               0.005 |
| concurrent_read_write | ducknng | rpc      | ipc       | arrow_ipc_stream    | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.645 |             0.051 |    400000 |       400000 |         4 |         7843137 |               7843137 |                 78.4 |                  0.014 |               0.026 |
| concurrent_read_write | ducknng | rpc      | ipc       | arrow_ipc_stream    | readers          |       2 |       0 |                    100000 |                     2 |        0.574 |             0.047 |    400000 |            0 |         0 |         8510638 |                     0 |                  0.0 |                  0.023 |               0.024 |
| concurrent_read_write | ducknng | rpc      | ipc       | arrow_ipc_stream    | writers          |       0 |       2 |                    100000 |                     2 |        0.531 |             0.004 |         0 |       400000 |         4 |               0 |             100000000 |               1000.0 |                  0.002 |               0.002 |
| concurrent_read_write | ducknng | rpc      | ipc       | ducknng_quack_batch | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.640 |             0.049 |    400000 |       400000 |         4 |         8163265 |               8163265 |                 81.6 |                  0.013 |               0.028 |
| concurrent_read_write | ducknng | rpc      | ipc       | ducknng_quack_batch | readers          |       2 |       0 |                    100000 |                     2 |        0.580 |             0.047 |    400000 |            0 |         0 |         8510638 |                     0 |                  0.0 |                  0.023 |               0.025 |
| concurrent_read_write | ducknng | rpc      | ipc       | ducknng_quack_batch | writers          |       0 |       2 |                    100000 |                     2 |        0.540 |             0.005 |         0 |       400000 |         4 |               0 |              80000000 |                800.0 |                  0.002 |               0.003 |
| concurrent_read_write | ducknng | rpc      | tcp       | arrow_ipc_stream    | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.637 |             0.052 |    400000 |       400000 |         4 |         7692308 |               7692308 |                 76.9 |                  0.019 |               0.029 |
| concurrent_read_write | ducknng | rpc      | tcp       | arrow_ipc_stream    | readers          |       2 |       0 |                    100000 |                     2 |        0.600 |             0.046 |    400000 |            0 |         0 |         8695652 |                     0 |                  0.0 |                  0.023 |               0.024 |
| concurrent_read_write | ducknng | rpc      | tcp       | arrow_ipc_stream    | writers          |       0 |       2 |                    100000 |                     2 |        0.552 |             0.004 |         0 |       400000 |         4 |               0 |             100000000 |               1000.0 |                  0.002 |               0.002 |
| concurrent_read_write | ducknng | rpc      | tcp       | ducknng_quack_batch | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.678 |             0.050 |    400000 |       400000 |         4 |         8000000 |               8000000 |                 80.0 |                  0.018 |               0.027 |
| concurrent_read_write | ducknng | rpc      | tcp       | ducknng_quack_batch | readers          |       2 |       0 |                    100000 |                     2 |        0.588 |             0.049 |    400000 |            0 |         0 |         8163265 |                     0 |                  0.0 |                  0.024 |               0.027 |
| concurrent_read_write | ducknng | rpc      | tcp       | ducknng_quack_batch | writers          |       0 |       2 |                    100000 |                     2 |        0.537 |             0.004 |         0 |       400000 |         4 |               0 |             100000000 |               1000.0 |                  0.002 |               0.003 |
| concurrent_read_write | ducknng | rpc      | ws        | arrow_ipc_stream    | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.621 |             0.054 |    400000 |       400000 |         4 |         7407407 |               7407407 |                 74.1 |                  0.014 |               0.029 |
| concurrent_read_write | ducknng | rpc      | ws        | arrow_ipc_stream    | readers          |       2 |       0 |                    100000 |                     2 |        0.583 |             0.052 |    400000 |            0 |         0 |         7692308 |                     0 |                  0.0 |                  0.026 |               0.028 |
| concurrent_read_write | ducknng | rpc      | ws        | arrow_ipc_stream    | writers          |       0 |       2 |                    100000 |                     2 |        0.514 |             0.005 |         0 |       400000 |         4 |               0 |              80000000 |                800.0 |                  0.002 |               0.003 |
| concurrent_read_write | ducknng | rpc      | ws        | ducknng_quack_batch | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.649 |             0.048 |    400000 |       400000 |         4 |         8333333 |               8333333 |                 83.3 |                  0.018 |               0.025 |
| concurrent_read_write | ducknng | rpc      | ws        | ducknng_quack_batch | readers          |       2 |       0 |                    100000 |                     2 |        0.589 |             0.039 |    400000 |            0 |         0 |        10256410 |                     0 |                  0.0 |                  0.019 |               0.024 |
| concurrent_read_write | ducknng | rpc      | ws        | ducknng_quack_batch | writers          |       0 |       2 |                    100000 |                     2 |        0.545 |             0.005 |         0 |       400000 |         4 |               0 |              80000000 |                800.0 |                  0.002 |               0.003 |

## HTTP ducknng vs Quack ratio

This is the apples-to-apples comparison on the common HTTP-facing path,
showing both ducknng serializer modes against Quack’s native HTTP path.

|     rows | ducknng_http_arrow_median_seconds | ducknng_http_quack_batch_median_seconds | quack_http_median_seconds | ducknng_arrow_over_quack_ratio | ducknng_quack_batch_over_quack_ratio |
|---------:|----------------------------------:|----------------------------------------:|--------------------------:|-------------------------------:|-------------------------------------:|
|   100000 |                             0.096 |                                   0.066 |                     0.086 |                          1.116 |                                0.767 |
|  1000000 |                             0.812 |                                   0.645 |                     0.685 |                          1.185 |                                0.942 |
| 10000000 |                             6.104 |                                   3.742 |                     4.591 |                          1.330 |                                0.815 |

## Notes

- The row benchmark uses a local TPC-H `lineitem` table generated
  through DuckDB’s `tpch` extension when needed.
- `quack` is installed from `core_nightly` during the run if it is not
  already available.
