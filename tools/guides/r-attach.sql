-- A DuckDB session attaches to the running R session through ducknng and
-- reads a data frame from it as Arrow.
SET VARIABLE reply = ducknng_request_raw(
  getvariable('r_url'),
  ducknng_encode_rpc_call('eval',
    '{"code":"data.frame(draw = seq_along(resamples), mean_mpg = resamples)"}'),
  30000, 0::UBIGINT);
SELECT count(*) AS draws,
       round(avg(mean_mpg), 2) AS mean_of_means,
       round(quantile_cont(mean_mpg, 0.025), 2) AS low_2_5,
       round(quantile_cont(mean_mpg, 0.975), 2) AS high_97_5
FROM ducknng_parse_body(ducknng_frame_payload(getvariable('reply')),
  'application/vnd.apache.arrow.stream');
