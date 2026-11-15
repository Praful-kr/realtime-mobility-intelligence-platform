"""Generates the provisioned Grafana dashboards (dashboards/grafana/*.json). Run: python dashboards/build_dashboards.py

Rules the checker (tests/e2e/observability_check.py) relies on: every panel carries real targets; a panel that is
legitimately empty in a healthy system must say "[optional]" in its description, everything else must return rows."""
from __future__ import annotations

import json
from pathlib import Path

PROM = {"type": "prometheus", "uid": "prometheus"}
PG = {"type": "grafana-postgresql-datasource", "uid": "postgres"}
OUT = Path(__file__).parent / "grafana"
R = "$__rate_interval"
# Headline rate/lag stats show the PEAK over the selected range: the "last value" of a finished run is just 0.
STAT_MAX = {"options": {"reduceOptions": {"calcs": ["max"], "fields": "", "values": False},
                        "colorMode": "value", "graphMode": "area"},
            "fieldConfig": {"defaults": {"color": {"mode": "fixed", "fixedColor": "blue"}}, "overrides": []}}
STAT_BLUE = {"fieldConfig": {"defaults": {"color": {"mode": "fixed", "fixedColor": "blue"}}, "overrides": []}}


def prom(expr, legend="", ref="A"):
    return {"refId": ref, "datasource": PROM, "expr": expr, "legendFormat": legend, "range": True, "instant": False}


def sql(q, fmt="time_series", ref="A"):
    return {"refId": ref, "datasource": PG, "rawSql": q, "format": fmt, "rawQuery": True, "editorMode": "code"}


class Board:
    def __init__(self, uid, title, time_from="now-15m", time_to="now", refresh="5s"):
        self.d = {"uid": uid, "title": title, "tags": ["mobility"], "timezone": "utc", "schemaVersion": 39,
                  "version": 1, "editable": False, "refresh": refresh, "time": {"from": time_from, "to": time_to},
                  "panels": [], "templating": {"list": []}, "annotations": {"list": []}}
        self._id, self._y, self._x, self._rowh = 0, 0, 0, 0

    def row(self, title):
        self._advance_row()
        self._id += 1
        self.d["panels"].append({"id": self._id, "type": "row", "title": title, "collapsed": False,
                                 "gridPos": {"h": 1, "w": 24, "x": 0, "y": self._y}})
        self._y += 1

    def _advance_row(self):
        if self._x:
            self._y += self._rowh
            self._x, self._rowh = 0, 0

    def panel(self, title, kind, targets, w=8, h=7, unit=None, desc="", ds=None, extra=None):
        if self._x + w > 24:
            self._advance_row()
        self._id += 1
        p = {"id": self._id, "type": kind, "title": title, "description": desc, "targets": targets,
             "datasource": ds or targets[0]["datasource"], "gridPos": {"h": h, "w": w, "x": self._x, "y": self._y},
             "fieldConfig": {"defaults": ({"unit": unit} if unit else {}), "overrides": []}, "options": {}}
        if kind == "timeseries":
            p["options"] = {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}
        if kind == "stat":
            p["options"] = {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                            "colorMode": "value", "graphMode": "area"}
        if extra:
            extra = dict(extra)
            fc = extra.pop("fieldConfig", None)
            if fc:   # merge, do not replace: keeps the unit set above
                p["fieldConfig"]["defaults"].update(fc.get("defaults", {}))
            p.update(extra)
        self.d["panels"].append(p)
        self._x += w
        self._rowh = max(self._rowh, h)

    def save(self, name):
        self._advance_row()
        OUT.mkdir(parents=True, exist_ok=True)
        (OUT / name).write_text(json.dumps(self.d, indent=1) + "\n")


def pipeline():
    b = Board("mobility-pipeline", "Pipeline Overview")
    b.row("Headline")
    b.panel("Events produced / s (peak)", "stat", [prom(f"sum(rate(producer_events_sent_total[{R}]))")], w=4, h=4,
            unit="ops", extra=STAT_MAX, desc="Peak replay-producer send rate in the range (host process, only while a replay runs).")
    b.panel("Silver events / s (peak)", "stat",
            [prom(f'sum(rate(spark_input_rows_total{{query="silver-trips"}}[{R}]))')], w=4, h=4, unit="ops", extra=STAT_MAX)
    b.panel("Kafka consumer lag (peak)", "stat", [prom('sum(kafka_topic_partition_current_offset{topic="trips.raw.v1"}) - sum(spark_kafka_processed_offset_sum{topic="trips.raw.v1"})')], w=4, h=4, unit="short", extra=STAT_MAX,
            desc="Peak of: records in the raw topic (broker log-end offset) minus the offset Bronze has processed.")
    b.panel("DLQ events (total)", "stat",
            [prom('sum(kafka_topic_partition_current_offset{topic="trips.dlq.v1"})')], w=4, h=4)
    b.panel("Kafka brokers", "stat", [prom("kafka_brokers")], w=4, h=4)
    b.panel("Targets up", "stat", [prom('sum(up{job!="producer"})')], w=4, h=4)
    b.row("Throughput")
    b.panel("Events processed per second, by streaming query", "timeseries",
            [prom(f"sum by (query) (rate(spark_input_rows_total[{R}]))", "{{query}}")], w=12, unit="ops")
    b.panel("Streaming throughput (Spark processedRowsPerSecond)", "timeseries",
            [prom("spark_processed_rows_per_second", "{{query}}")], w=12, unit="ops")
    b.row("Lag and latency")
    b.panel("Kafka consumer lag (records behind)", "timeseries",
            [prom('sum(kafka_topic_partition_current_offset{topic="trips.raw.v1"}) - sum(spark_kafka_processed_offset_sum{topic="trips.raw.v1"})', "bronze-trips-raw lag")], w=6,
            desc="True lag = broker log-end offset - Spark processed offset (Spark never commits to a consumer group).")
    b.panel("Delta version lag (Silver, Gold)", "timeseries",
            [prom("spark_delta_version_lag", "{{query}}")], w=6,
            desc="Table versions written upstream but not yet consumed.")
    b.panel("Processing latency: producer emit -> Silver (s)", "timeseries",
            [prom("spark_silver_latency_seconds", "{{stat}}")], w=6, unit="s")
    b.panel("Micro-batch duration (ms)", "timeseries", [prom("spark_batch_duration_ms", "{{query}}")], w=6, unit="ms")
    b.row("Failures and data quality")
    b.panel("Failed / rejected events per second", "timeseries", [
        prom(f"sum(rate(producer_events_failed_total[{R}])) or vector(0)", "producer delivery failures", "A"),
        prom(f'sum(rate(spark_silver_rows_total{{outcome="quarantined"}}[{R}])) or vector(0)', "quarantined (rule violations)", "B"),
        prom(f'sum(rate(spark_silver_rows_total{{outcome="undecodable"}}[{R}])) or vector(0)', "undecodable payloads", "C")],
        w=8, unit="ops")
    b.panel("DLQ volume", "timeseries", [
        prom('sum(kafka_topic_partition_current_offset{topic="trips.dlq.v1"})', "events in DLQ topic", "A")], w=8)
    b.panel("Silver outcomes per second", "timeseries",
            [prom(f"sum by (outcome) (rate(spark_silver_rows_total[{R}]))", "{{outcome}}")], w=8, unit="ops")
    b.panel("Late data dropped by watermark (groups)", "timeseries",
            [prom(f"sum(increase(spark_dropped_by_watermark_groups_total[{R}])) or vector(0)", "dropped groups")],
            w=8, desc="Lower bound on events lost to lateness; exact loss = Silver minus Gold (gold_verify).")
    b.panel("Backpressure: producer queue depth", "timeseries", [
        prom("producer_queue_depth", "queue depth", "A"),
        prom(f"sum(rate(producer_backpressure_events_total[{R}])) or vector(0)", "backpressure events/s", "B")], w=8)
    b.row("Health")
    b.panel("Kafka: under-replicated partitions", "timeseries",
            [prom("sum(kafka_topic_partition_under_replicated_partition)", "under-replicated"),
             prom("kafka_brokers", "brokers", "B")], w=8)
    b.panel("Kafka events written per topic", "timeseries",
            [prom("sum by (topic) (kafka_topic_partition_current_offset)", "{{topic}}")], w=8)
    b.panel("Scrape targets", "timeseries", [prom("up", "{{job}}")], w=8)
    b.panel("Stateful operator rows (watermark-bounded)", "timeseries",
            [prom("spark_state_rows", "{{query}}")], w=12)
    b.panel("Kafka produce rate by topic (events/s)", "timeseries",
            [prom(f"sum by (topic) (rate(kafka_topic_partition_current_offset[{R}]))", "{{topic}}")], w=12, unit="ops")
    b.save("pipeline.json")


def api():
    b = Board("mobility-api", "API")
    b.row("Traffic")
    b.panel("Request rate by route", "timeseries",
            [prom(f"sum by (route) (rate(api_http_requests_total[{R}]))", "{{route}}")], w=12, unit="reqps")
    b.panel("Latency p50 / p95 / p99", "timeseries", [
        prom(f"histogram_quantile(0.5, sum by (le) (rate(api_http_request_duration_seconds_bucket[{R}])))", "p50", "A"),
        prom(f"histogram_quantile(0.95, sum by (le) (rate(api_http_request_duration_seconds_bucket[{R}])))", "p95", "B"),
        prom(f"histogram_quantile(0.99, sum by (le) (rate(api_http_request_duration_seconds_bucket[{R}])))", "p99", "C")],
        w=12, unit="s")
    b.row("Errors and saturation")
    b.panel("5xx and 4xx per second", "timeseries", [
        prom(f'sum(rate(api_http_requests_total{{status=~"5.."}}[{R}])) or vector(0)', "5xx", "A"),
        prom(f'sum(rate(api_http_requests_total{{status=~"4.."}}[{R}])) or vector(0)', "4xx", "B")], w=8, unit="reqps")
    b.panel("In-flight requests", "timeseries", [prom("api_http_requests_in_flight", "in flight")], w=8)
    b.panel("Database-unavailable responses", "timeseries",
            [prom(f"sum(increase(api_db_errors_total[{R}])) or vector(0)", "db errors")], w=8)
    b.panel("API up", "stat", [prom('up{job="api"}')], w=6, h=4)
    b.save("api.json")


def business():
    b = Board("mobility-business", "Mobility Demand (Gold)", time_from="2024-01-01T00:00:00.000Z",
              time_to="2024-01-04T16:30:00.000Z", refresh="")
    tf = "$__timeFilter(window_start)"
    b.row("Demand and revenue")
    b.panel("Trips per minute", "timeseries",
            [sql(f"SELECT window_start AS time, SUM(trips) AS trips FROM gold.trips_per_minute WHERE {tf} GROUP BY 1 ORDER BY 1")], w=12)
    b.panel("Revenue per hour ($)", "timeseries",
            [sql(f"SELECT window_start AS time, SUM(revenue) AS revenue FROM gold.hourly_demand WHERE {tf} GROUP BY 1 ORDER BY 1")],
            w=12, unit="currencyUSD")
    b.panel("Rolling 15-minute demand (5-minute slide)", "timeseries",
            [sql(f"SELECT window_start AS time, trips FROM gold.demand_sliding_15m WHERE {tf} ORDER BY 1")], w=12)
    b.panel("Average trip duration per hour (min)", "timeseries",
            [sql(f"SELECT date_trunc('hour', window_start) AS time, SUM(duration_sum_s)::float / NULLIF(SUM(duration_n), 0) / 60 AS avg_minutes "
                 f"FROM gold.trips_per_minute WHERE {tf} GROUP BY 1 ORDER BY 1")], w=12)
    b.row("Where")
    zone = ("SELECT max(zone_name) AS zone, max(borough) AS borough, SUM(trips) AS trips FROM gold.zone_demand_5m "
            "WHERE role = '{role}' AND " + tf + " GROUP BY zone_id ORDER BY trips DESC LIMIT 15")
    b.panel("Top 15 pickup zones", "table", [sql(zone.format(role="pickup"), "table")], w=8, h=9)
    b.panel("Top 15 drop-off zones", "table", [sql(zone.format(role="dropoff"), "table")], w=8, h=9)
    b.panel("Pickups by borough", "piechart",
            [sql("SELECT COALESCE(borough, 'Unknown') AS borough, SUM(trips) AS trips FROM gold.zone_demand_5m "
                 f"WHERE role = 'pickup' AND {tf} GROUP BY 1 ORDER BY 2 DESC", "table")], w=8, h=9,
            extra={"options": {"reduceOptions": {"calcs": ["lastNotNull"], "values": True, "fields": "/^trips$/"},
                               "pieType": "donut", "legend": {"displayMode": "table", "placement": "right"}}})
    b.row("Hour of day and anomalies")
    b.panel("Demand by hour of day", "barchart",
            [sql("SELECT hour_of_day::text AS hour, SUM(trips) AS trips FROM gold.v_demand_by_hour_of_day GROUP BY hour_of_day ORDER BY hour_of_day", "table")],
            w=12, extra={"options": {"xField": "hour", "legend": {"displayMode": "list"}}})
    b.panel("Trip outliers per minute, by type", "timeseries",
            [sql(f"SELECT window_start AS time, anomaly_type AS metric, SUM(n) AS n FROM gold.anomaly_counts WHERE {tf} GROUP BY 1, 2 ORDER BY 1")],
            w=12, desc="Speed > 80 mph, fare/mile > $50, tip > $100, duration > 4 h.")
    b.panel("Demand anomalies (|z| > 3 vs trailing hour)", "table",
            [sql(f"SELECT window_start AS time, trips, round(trailing_mean::numeric, 1) AS trailing_mean, round(z_score::numeric, 1) AS z "
                 f"FROM gold.v_demand_anomalies WHERE {tf} ORDER BY abs(z_score) DESC LIMIT 10", "table")], w=12)
    b.panel("Total trips / revenue in range", "stat",
            [sql(f"SELECT SUM(trips) AS trips FROM gold.hourly_demand WHERE {tf}", "table", "A"),
             sql(f"SELECT SUM(revenue) AS revenue FROM gold.hourly_demand WHERE {tf}", "table", "B")], w=12, h=4, extra=STAT_BLUE)
    b.save("business.json")


if __name__ == "__main__":
    pipeline()
    api()
    business()
    print("wrote", sorted(p.name for p in OUT.glob("*.json")))
