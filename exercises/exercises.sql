-- =============================================================================
-- MODULE 08: Hands-On Exercises
-- HCI Pharma — Apache Flink on Confluent Cloud Training
--
-- INSTRUCTIONS:
--   Write your SQL below each exercise prompt.
--   Full solutions are in solutions/solutions.sql
--
-- BEFORE YOU START:
--   Ensure you have run 00-setup/00-setup.sql and that data is flowing
--   into the three source tables (products_t, prices_t, alerts_t).
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- EXERCISE 01  ⭐  Data Transformation
-- ─────────────────────────────────────────────────────────────────────────────
-- SCENARIO:
--   The BAG compliance team needs a formatted price change report.
--   Write a SELECT query against prices_t that returns:
--     - gtin
--     - previous_public_price_chf
--     - new_public_price_chf
--     - delta_chf: the absolute change, rounded to 2 decimal places
--     - change_pct: the % change rounded to 2 decimal places
--                  (NULL if previous_public_price_chf is 0)
--     - direction: 'INCREASE', 'DECREASE', or 'UNCHANGED'
--     - effective_date: effective_date_utc formatted as 'yyyy-MM-dd'
--     - change_reason (uppercased)
--
-- EXPECTED: Each price update row enriched with the derived columns above.
-- HINT: Use ROUND(), CASE, DATE_FORMAT(), TO_TIMESTAMP_LTZ(), UPPER()

-- YOUR ANSWER:


-- ─────────────────────────────────────────────────────────────────────────────
-- EXERCISE 02  ⭐  Filtering — Narcotics Stream
-- ─────────────────────────────────────────────────────────────────────────────
-- SCENARIO:
--   The hospital pharmacy integration team needs a real-time stream of
--   only ACTIVE narcotic products.
--
--   Write a SELECT query (or CREATE VIEW) that returns from products_t:
--     - gtin
--     - product_name
--     - active_substance
--     - narcotics_category
--     - public_price_chf
--     - last updated as a formatted timestamp ('yyyy-MM-dd HH:mm:ss')
--
--   Filter: only ACTIVE products with narcotics_category IS NOT NULL
--
-- EXPECTED: Only BetmG-controlled products with ACTIVE marketing status.

-- YOUR ANSWER:


-- ─────────────────────────────────────────────────────────────────────────────
-- EXERCISE 03  ⭐  Aggregation — Alert Counts by ATC Class
-- ─────────────────────────────────────────────────────────────────────────────
-- SCENARIO:
--   The medical affairs team wants a real-time count of drug alerts
--   grouped by ATC level-1 therapeutic class (first character of atc_code).
--
--   Join alerts_t with products_t and GROUP BY to produce:
--     - atc_class: first character of atc_code (e.g. 'N', 'A', 'J')
--     - alert_type
--     - alert_count: total alerts
--     - affected_products: count of distinct GTINs
--
-- EXPECTED: Running total per (atc_class, alert_type), updated as new alerts arrive.
-- HINT: SUBSTRING(atc_code, 1, 1), COUNT(DISTINCT ...)

-- YOUR ANSWER:


-- ─────────────────────────────────────────────────────────────────────────────
-- EXERCISE 04  ⭐  HAVING — Price-Volatile Products
-- ─────────────────────────────────────────────────────────────────────────────
-- SCENARIO:
--   Identify products that have had more than 2 price changes AND whose
--   average percentage change exceeds 5%.
--
--   Query prices_t with GROUP BY gtin and return:
--     - gtin
--     - update_count
--     - avg_change_pct (rounded to 2dp)
--     - max_change_pct
--
--   Filter with HAVING for update_count > 2 AND avg_change_pct > 5.0
--
-- EXPECTED: Only highly volatile products appear in the result.
-- HINT: Use HAVING with multiple conditions

-- YOUR ANSWER:


-- ─────────────────────────────────────────────────────────────────────────────
-- EXERCISE 05  ⭐⭐  INSERT INTO — CLASS_I Recall Pipeline
-- ─────────────────────────────────────────────────────────────────────────────
-- SCENARIO:
--   Create a sink table and a continuous INSERT INTO job that writes
--   only CLASS_I recalls to a dedicated Kafka topic 'hci.flink.class-i-recalls'.
--
--   Step 1: CREATE TABLE class_i_recalls_sink with columns:
--             alert_id (STRING, PRIMARY KEY NOT ENFORCED)
--             gtin (STRING)
--             severity (STRING)
--             headline (STRING)
--             lot_count (BIGINT) — CARDINALITY(affected_lots)
--             issued_cet (STRING) — formatted timestamp
--             source_url (STRING)
--
--   Step 2: INSERT INTO class_i_recalls_sink from alerts_t
--           WHERE alert_type = 'RECALL' AND severity = 'CLASS_I'
--
-- EXPECTED: A running Flink job writing CLASS_I recalls to the sink topic.
-- HINT: Set 'pipeline.name' before the INSERT

-- YOUR ANSWER (Step 1 — CREATE TABLE):


-- YOUR ANSWER (Step 2 — INSERT INTO):


-- ─────────────────────────────────────────────────────────────────────────────
-- EXERCISE 06  ⭐⭐  Regular Join — Shortage Enrichment
-- ─────────────────────────────────────────────────────────────────────────────
-- SCENARIO:
--   The wholesale distribution system needs shortage alerts enriched with
--   product details so it can identify alternative suppliers.
--
--   Join alerts_t (WHERE alert_type = 'SHORTAGE') with products_t and return:
--     - alert_id
--     - gtin
--     - severity
--     - headline
--     - product_name (from products_t, COALESCE to 'UNKNOWN')
--     - manufacturer
--     - active_substance
--     - atc_code
--     - marketing_status (current status of the product)
--     - issued_cet (formatted)
--     - expires_or_status: if expires_utc IS NULL then 'INDEFINITE'
--                          else the formatted date
--
-- EXPECTED: Shortage alerts enriched with full product context.

-- YOUR ANSWER:


-- ─────────────────────────────────────────────────────────────────────────────
-- EXERCISE 07  ⭐⭐  Temporal Join — Historical Product Status at Alert Time
-- ─────────────────────────────────────────────────────────────────────────────
-- SCENARIO:
--   For regulatory audit purposes, determine the MARKETING STATUS of each
--   product AT THE TIME the alert was issued (not the current status).
--
--   Use a temporal join between alerts_t and products_versioned_t
--   (FOR SYSTEM_TIME AS OF a.event_time).
--
--   Return:
--     - alert_id
--     - gtin
--     - alert_type
--     - severity
--     - product_name
--     - status_at_alert_time: marketing_status from the temporal join
--     - current_public_price: public_price_chf at that point in time
--     - alert_issued_at: formatted timestamp
--
-- HINT: products_versioned_t must have PRIMARY KEY declared.
--       Syntax: LEFT JOIN products_versioned_t FOR SYSTEM_TIME AS OF a.event_time AS p

-- YOUR ANSWER:


-- ─────────────────────────────────────────────────────────────────────────────
-- EXERCISE 08  ⭐⭐  Tumbling Window — Hourly Alert Summary by Severity
-- ─────────────────────────────────────────────────────────────────────────────
-- SCENARIO:
--   Build a 1-hour tumbling window query over alerts_t that produces:
--     - window_start
--     - window_end
--     - severity
--     - alert_count: total alerts in the window
--     - affected_gtins: count of distinct GTINs
--     - recall_count: count where alert_type = 'RECALL'
--
--   ORDER BY window_start DESC, alert_count DESC
--
-- EXPECTED: Hourly buckets with alert volume per severity level.
-- HINT: TABLE(TUMBLE(TABLE alerts_t, DESCRIPTOR(event_time), INTERVAL '1' HOUR))

-- YOUR ANSWER:


-- ─────────────────────────────────────────────────────────────────────────────
-- EXERCISE 09  ⭐⭐  Hopping Window — 7-Day Rolling Shortage Count per ATC
-- ─────────────────────────────────────────────────────────────────────────────
-- SCENARIO:
--   The procurement planning team wants a rolling 7-day shortage count per
--   ATC level-2 class (first 3 characters of atc_code), updated every day.
--
--   Join alerts_t (WHERE alert_type = 'SHORTAGE') with products_t, then apply
--   a hopping window:
--     SIZE = 7 DAYS, ADVANCE BY = 1 DAY
--
--   Return:
--     - window_start
--     - window_end
--     - atc_level2 (SUBSTRING(atc_code, 1, 3))
--     - shortage_count
--     - affected_products (COUNT DISTINCT gtin)
--
-- HINT: HOP(TABLE source, DESCRIPTOR(time_col), INTERVAL '1' DAY, INTERVAL '7' DAY)

-- YOUR ANSWER:


-- ─────────────────────────────────────────────────────────────────────────────
-- EXERCISE 10  ⭐⭐  UNNEST — Per-Lot Recall Report
-- ─────────────────────────────────────────────────────────────────────────────
-- SCENARIO:
--   Hospital pharmacies need a report listing each affected lot number
--   individually for CLASS_I and CLASS_II recalls, enriched with the
--   product name and active substance.
--
--   Using CROSS JOIN UNNEST(affected_lots), produce one row per lot:
--     - alert_id
--     - gtin
--     - product_name (from products_t, COALESCE 'UNKNOWN')
--     - active_substance
--     - narcotics_category
--     - severity
--     - lot_number (the individual lot from the array)
--     - is_swiss_lot: TRUE if lot LIKE 'CH-%'
--     - recall_date: formatted
--
--   Only include recalls with CARDINALITY(affected_lots) > 0
--
-- HINT: CROSS JOIN UNNEST(a.affected_lots) AS t(lot_number)

-- YOUR ANSWER:


-- ─────────────────────────────────────────────────────────────────────────────
-- EXERCISE 11  ⭐⭐⭐  OVER Aggregation — Price History with Ranking
-- ─────────────────────────────────────────────────────────────────────────────
-- SCENARIO:
--   Produce a price history report for each GTIN showing:
--     - gtin
--     - effective_date (formatted 'yyyy-MM-dd')
--     - new_public_price_chf
--     - change_reason
--     - running_avg_price: average of all prices up to and including this row
--                          (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
--     - price_rank: RANK() of this price update per GTIN ordered by
--                   new_public_price_chf DESC (highest price = rank 1)
--     - update_number: ROW_NUMBER() per GTIN ordered by event_time ASC
--
-- HINT: You'll need multiple OVER clauses.
--       RANK() OVER (PARTITION BY gtin ORDER BY new_public_price_chf DESC)

-- YOUR ANSWER:


-- ─────────────────────────────────────────────────────────────────────────────
-- EXERCISE 12  ⭐⭐⭐  MATCH_RECOGNIZE — Shortage → Recall Pattern
-- ─────────────────────────────────────────────────────────────────────────────
-- SCENARIO:
--   Detect products where a SHORTAGE alert is followed by a RECALL
--   within 60 days — indicating a supply problem that escalated to a safety
--   issue. This is a key pharmacovigilance signal.
--
--   Use MATCH_RECOGNIZE on alerts_t to detect: SHORTAGE → RECALL
--     PARTITION BY gtin
--     ORDER BY event_time
--     WITHIN INTERVAL '60' DAY
--
--   Measures to extract:
--     - gtin
--     - shortage_alert_id
--     - recall_alert_id
--     - shortage_headline
--     - recall_headline
--     - shortage_date (formatted 'yyyy-MM-dd')
--     - recall_date (formatted 'yyyy-MM-dd')
--     - days_between: TIMESTAMPDIFF(DAY, ...)
--
--   Use ONE ROW PER MATCH and AFTER MATCH SKIP TO NEXT ROW.
--
-- HINT: DEFINE S AS S.alert_type = 'SHORTAGE', R AS R.alert_type = 'RECALL'
--       PATTERN (S R)

-- YOUR ANSWER:
