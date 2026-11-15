-- Serving schema. Idempotent (re-apply with `make pg-apply`). Windows are UTC.
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.trips_per_minute (
  window_start timestamptz NOT NULL, taxi_type text NOT NULL,
  trips bigint NOT NULL, revenue double precision, fare_sum double precision, tip_sum double precision,
  distance_sum double precision, duration_sum_s bigint, duration_n bigint,
  avg_duration_s double precision, avg_fare double precision,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (window_start, taxi_type));

CREATE TABLE IF NOT EXISTS gold.demand_sliding_15m (
  window_start timestamptz NOT NULL PRIMARY KEY, window_end timestamptz NOT NULL,
  trips bigint NOT NULL, revenue double precision, updated_at timestamptz NOT NULL DEFAULT now());

CREATE TABLE IF NOT EXISTS gold.zone_demand_5m (
  window_start timestamptz NOT NULL, zone_id int NOT NULL, role text NOT NULL,
  borough text, zone_name text, trips bigint NOT NULL, revenue double precision,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (window_start, zone_id, role));
CREATE INDEX IF NOT EXISTS zone_demand_zone_idx ON gold.zone_demand_5m (zone_id, role);

CREATE TABLE IF NOT EXISTS gold.hourly_demand (
  window_start timestamptz NOT NULL, taxi_type text NOT NULL,
  trips bigint NOT NULL, revenue double precision, fare_sum double precision,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (window_start, taxi_type));

CREATE TABLE IF NOT EXISTS gold.anomaly_counts (
  window_start timestamptz NOT NULL, anomaly_type text NOT NULL, n bigint NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (window_start, anomaly_type));

CREATE TABLE IF NOT EXISTS gold.stream_metrics (
  id bigserial PRIMARY KEY, ts timestamptz NOT NULL DEFAULT now(),
  query_name text NOT NULL, run_id text, batch_id bigint, input_rows bigint,
  dropped_by_watermark bigint, state_rows bigint, trigger_ms bigint, processed_rows_per_s double precision);

-- Demand by hour of day (NYC wall-clock, see DEVELOPMENT.md on timestamps)
CREATE OR REPLACE VIEW gold.v_demand_by_hour_of_day AS
SELECT EXTRACT(HOUR FROM window_start AT TIME ZONE 'UTC')::int AS hour_of_day, taxi_type,
       SUM(trips) AS trips, SUM(revenue) AS revenue,
       COUNT(DISTINCT (window_start AT TIME ZONE 'UTC')::date) AS days_observed
FROM gold.hourly_demand GROUP BY 1, 2;

-- Demand anomalies: |z| > 3 vs the trailing 60 minutes of per-minute demand (all taxi types)
CREATE OR REPLACE VIEW gold.v_demand_anomalies AS
WITH per_min AS (SELECT window_start, SUM(trips) AS trips FROM gold.trips_per_minute GROUP BY 1),
stats AS (
  SELECT window_start, trips,
         AVG(trips) OVER w AS trailing_mean, STDDEV_SAMP(trips) OVER w AS trailing_sd
  FROM per_min
  WINDOW w AS (ORDER BY window_start RANGE BETWEEN INTERVAL '60 minutes' PRECEDING AND INTERVAL '1 minute' PRECEDING))
SELECT window_start, trips, trailing_mean, trailing_sd,
       (trips - trailing_mean) / NULLIF(trailing_sd, 0) AS z_score
FROM stats WHERE trailing_sd > 0 AND ABS((trips - trailing_mean) / trailing_sd) > 3;
