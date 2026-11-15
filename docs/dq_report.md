# Data Quality Report

_Generated 2026-11-15T08:02:06+00:00 from Delta tables (silver, quarantine, dq_metrics); 1 micro-batches._

## Summary

| Metric | Value |
|---|---:|
| Records received (Bronze) | 305,998 |
| Quarantined (ERROR rules / undecodable) | 4,660 (1.52%) |
|   of which undecodable | 300 |
| Duplicate deliveries dropped | 5,919 (1.93%) |
| Accepted into Silver (distinct events) | 295,419 |
| Silver rows with zero warnings (score 1.0) | 271,925 (92.05%) |
| Mean Silver DQ score | 0.9906 |
| Reconciliation: received = quarantined + dropped dups + inserted | OK |

### Quarantine reasons (a record can hit several)

| Rule | Category | Rows |
|---|---|---:|
| `negative_fare` | value | 4,317 |
| `negative_total` | value | 4,200 |
| `undecodable_payload` | schema | 300 |
| `dropoff_before_pickup` | timestamp | 10 |
| `implausible_distance` | business | 10 |
| `pickup_out_of_range` | timestamp | 5 |
| `trip_too_long` | business | 2 |
| `implausible_fare` | business | 1 |

### Warnings on accepted Silver rows (reduce DQ score by 0.1 each)

| Rule | Category | Rows |
|---|---|---:|
| `null_passenger_count` | null | 16,299 |
| `zero_distance` | value | 8,030 |
| `zero_passenger_count` | value | 2,914 |
| `null_payment_type` | null | 394 |

## Rule catalog

| Rule | Severity | Category | Description |
|---|---|---|---|
| `undecodable_payload` | error | schema | Payload is null, lacks the 0x00 magic byte, or is not valid Avro for the registered schema |
| `invalid_taxi_type` | error | schema | taxi_type is not yellow|green |
| `pickup_out_of_range` | error | timestamp | pickup before DQ_MIN_PICKUP_TS or more than DQ_MAX_FUTURE_DAYS after ingestion |
| `dropoff_before_pickup` | error | timestamp | dropoff earlier than pickup |
| `trip_too_long` | error | business | trip duration exceeds DQ_MAX_TRIP_SECONDS |
| `null_fare` | error | null | fare_amount is null |
| `negative_fare` | error | value | fare_amount < 0 |
| `negative_total` | error | value | total_amount < 0 |
| `negative_distance` | error | value | trip_distance < 0 |
| `implausible_distance` | error | business | trip_distance > DQ_MAX_DISTANCE_MI |
| `implausible_fare` | error | business | fare_amount > DQ_MAX_FARE |
| `invalid_pickup_zone` | error | business | pu_location_id null or outside 1..265 |
| `invalid_dropoff_zone` | error | business | do_location_id null or outside 1..265 |
| `invalid_passenger_count` | error | value | passenger_count outside 0..9 |
| `missing_dropoff` | warn | null | dropoff_ts is null |
| `null_passenger_count` | warn | null | passenger_count is null |
| `zero_passenger_count` | warn | value | passenger_count = 0 |
| `zero_distance` | warn | value | trip_distance = 0 |
| `null_payment_type` | warn | null | payment_type is null |
| `total_less_than_fare` | warn | business | total_amount < fare_amount - 0.01 |
