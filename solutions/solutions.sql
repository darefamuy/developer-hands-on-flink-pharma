-- =============================================================================
-- Exercise Solutions
-- Pharma — Apache Flink on Confluent Cloud Training
--
-- Reference solutions for all 12 exercises.
-- Try to complete the exercises independently before reading these.
-- =============================================================================

-- =============================================================================
-- SOLUTION 01  ⭐  Data Transformation — Price Change Report
-- =============================================================================
SELECT
    gtin,
    previous_public_price_chf,
    new_public_price_chf,
    ROUND(new_public_price_chf - previous_public_price_chf, 2)          AS delta_chf,
    CASE
        WHEN previous_public_price_chf = 0 THEN NULL
        ELSE ROUND(
            (new_public_price_chf - previous_public_price_chf)
            / previous_public_price_chf * 100, 2)
    END                                                                 AS change_pct,
    CASE
        WHEN new_public_price_chf > previous_public_price_chf THEN 'INCREASE'
        WHEN new_public_price_chf < previous_public_price_chf THEN 'DECREASE'
        ELSE 'UNCHANGED'
    END                                                                 AS direction,
    DATE_FORMAT(
        TO_TIMESTAMP_LTZ(effective_date_utc, 3),
        'yyyy-MM-dd')                                                   AS effective_date,
    UPPER(change_reason)                                                AS change_reason
FROM prices_t
LIMIT 20;


-- =============================================================================
-- SOLUTION 02  ⭐  Filtering — Active Narcotics Stream
-- =============================================================================
CREATE VIEW IF NOT EXISTS active_narcotics_v AS
SELECT
    gtin,
    product_name,
    active_substance,
    narcotics_category,
    public_price_chf,
    DATE_FORMAT(
        TO_TIMESTAMP_LTZ(last_updated_utc, 3),
        'yyyy-MM-dd HH:mm:ss')                  AS last_updated_cet
FROM products_t
WHERE marketing_status    = 'ACTIVE'
  AND narcotics_category IS NOT NULL;

SELECT * FROM active_narcotics_v LIMIT 10;


-- =============================================================================
-- SOLUTION 03  ⭐  Aggregation — Alert Counts by ATC Level-1 Class
-- =============================================================================
SELECT
    COALESCE(SUBSTRING(p.atc_code, 1, 1), 'UNKNOWN')   AS atc_class,
    a.alert_type,
    COUNT(*)                                            AS alert_count,
    COUNT(DISTINCT a.gtin)                              AS affected_products
FROM alerts_t AS a
LEFT JOIN products_t AS p ON a.gtin = p.gtin
GROUP BY
    COALESCE(SUBSTRING(p.atc_code, 1, 1), 'UNKNOWN'),
    a.alert_type;


-- =============================================================================
-- SOLUTION 04  ⭐  HAVING — Price-Volatile Products
-- =============================================================================
SELECT
    gtin,
    COUNT(*)                                    AS update_count,
    ROUND(AVG(
        CASE WHEN previous_public_price_chf > 0
             THEN ABS(new_public_price_chf - previous_public_price_chf)
                  / previous_public_price_chf * 100
        END), 2)                                AS avg_change_pct,
    ROUND(MAX(
        CASE WHEN previous_public_price_chf > 0
             THEN ABS(new_public_price_chf - previous_public_price_chf)
                  / previous_public_price_chf * 100
        END), 2)                                AS max_change_pct
FROM prices_t
GROUP BY gtin
HAVING COUNT(*) > 2
   AND AVG(
        CASE WHEN previous_public_price_chf > 0
             THEN ABS(new_public_price_chf - previous_public_price_chf)
                  / previous_public_price_chf * 100
        END) > 5.0;


-- =============================================================================
-- SOLUTION 05  ⭐⭐  INSERT INTO — CLASS_I Recall Pipeline
-- =============================================================================

-- Step 1: Create the sink table
CREATE TABLE IF NOT EXISTS class_i_recalls_sink (
    alert_id    STRING,
    gtin        STRING,
    severity    STRING,
    headline    STRING,
    lot_count   BIGINT,
    issued_cet  STRING,
    source_url  STRING,
    PRIMARY KEY (alert_id) NOT ENFORCED
) WITH (
    'connector'     = 'confluent',
    'kafka.topic'   = 'hci.flink.class-i-recalls',
    'value.format'  = 'json',
    'scan.startup.mode' = 'earliest-offset'
);

-- Step 2: Continuous insert job
SET 'pipeline.name' = 'hci-class-i-recall-pipeline';

INSERT INTO class_i_recalls_sink
SELECT
    alert_id,
    gtin,
    severity,
    headline,
    CARDINALITY(affected_lots)                                      AS lot_count,
    DATE_FORMAT(event_time, 'yyyy-MM-dd HH:mm:ss')                  AS issued_cet,
    source_url
FROM alerts_t
WHERE alert_type = 'RECALL'
  AND severity   = 'CLASS_I';


-- =============================================================================
-- SOLUTION 06  ⭐⭐  Regular Join — Shortage Enrichment
-- =============================================================================
SELECT
    a.alert_id,
    a.gtin,
    a.severity,
    a.headline,
    COALESCE(p.product_name, 'UNKNOWN')                             AS product_name,
    COALESCE(p.manufacturer, 'UNKNOWN')                             AS manufacturer,
    COALESCE(p.active_substance, 'UNKNOWN')                         AS active_substance,
    COALESCE(p.atc_code, 'UNKNOWN')                                 AS atc_code,
    COALESCE(p.marketing_status, 'UNKNOWN')                         AS marketing_status,
    DATE_FORMAT(a.event_time, 'yyyy-MM-dd HH:mm:ss')                AS issued_cet,
    CASE
        WHEN a.expires_utc IS NULL
        THEN 'INDEFINITE'
        ELSE DATE_FORMAT(TO_TIMESTAMP_LTZ(a.expires_utc, 3), 'yyyy-MM-dd')
    END                                                             AS expires_or_status
FROM alerts_t AS a
LEFT JOIN products_t AS p ON a.gtin = p.gtin
WHERE a.alert_type = 'SHORTAGE'
LIMIT 20;


-- =============================================================================
-- SOLUTION 07  ⭐⭐  Temporal Join — Historical Product Status at Alert Time
-- =============================================================================

-- Ensure products_versioned_t exists (from 04-joins.sql setup)
-- If not, create it:
CREATE TABLE IF NOT EXISTS products_versioned_t (
    gtin                    STRING,
    product_name            STRING,
    atc_code                STRING,
    manufacturer            STRING,
    active_substance        STRING,
    marketing_status        STRING,
    public_price_chf        DOUBLE,
    ex_factory_price_chf    DOUBLE,
    narcotics_category      STRING,
    last_updated_utc        BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(last_updated_utc, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND,
    PRIMARY KEY (gtin) NOT ENFORCED
) WITH (
    'connector'                 = 'confluent',
    'kafka.topic'               = 'hci.medicinal_products',
    'value.format'              = 'avro-confluent',
    'value.avro-confluent.url'  = '${schema.registry.url}',
    'scan.startup.mode'         = 'earliest-offset'
);

SELECT
    a.alert_id,
    a.gtin,
    a.alert_type,
    a.severity,
    COALESCE(p.product_name, 'UNKNOWN')                             AS product_name,
    COALESCE(p.marketing_status, 'UNKNOWN')                         AS status_at_alert_time,
    COALESCE(p.public_price_chf, 0.0)                               AS current_public_price,
    DATE_FORMAT(a.event_time, 'yyyy-MM-dd HH:mm:ss')                AS alert_issued_at
FROM alerts_t AS a
LEFT JOIN products_versioned_t FOR SYSTEM_TIME AS OF a.event_time AS p
    ON a.gtin = p.gtin
LIMIT 10;


-- =============================================================================
-- SOLUTION 08  ⭐⭐  Tumbling Window — Hourly Alert Summary by Severity
-- =============================================================================
SELECT
    window_start,
    window_end,
    severity,
    COUNT(*)                                        AS alert_count,
    COUNT(DISTINCT gtin)                            AS affected_gtins,
    COUNT(CASE WHEN alert_type = 'RECALL' THEN 1 END) AS recall_count
FROM TABLE(
    TUMBLE(TABLE alerts_t, DESCRIPTOR(event_time), INTERVAL '1' HOUR)
)
GROUP BY window_start, window_end, severity
ORDER BY window_start DESC, alert_count DESC;


-- =============================================================================
-- SOLUTION 09  ⭐⭐  Hopping Window — 7-Day Rolling Shortage Count per ATC
-- =============================================================================
SELECT
    window_start,
    window_end,
    COALESCE(SUBSTRING(p.atc_code, 1, 3), 'UNKNOWN')   AS atc_level2,
    COUNT(*)                                            AS shortage_count,
    COUNT(DISTINCT a.gtin)                              AS affected_products
FROM TABLE(
    HOP(TABLE alerts_t, DESCRIPTOR(event_time),
        INTERVAL '1' DAY,
        INTERVAL '7' DAY)
) AS a
LEFT JOIN products_t AS p ON a.gtin = p.gtin
WHERE a.alert_type = 'SHORTAGE'
GROUP BY
    window_start,
    window_end,
    COALESCE(SUBSTRING(p.atc_code, 1, 3), 'UNKNOWN')
ORDER BY window_start DESC, shortage_count DESC;


-- =============================================================================
-- SOLUTION 10  ⭐⭐  UNNEST — Per-Lot Recall Report
-- =============================================================================
SELECT
    a.alert_id,
    a.gtin,
    COALESCE(p.product_name, 'UNKNOWN')                             AS product_name,
    COALESCE(p.active_substance, 'UNKNOWN')                         AS active_substance,
    p.narcotics_category,
    a.severity,
    lot_number,
    lot_number LIKE 'CH-%'                                          AS is_swiss_lot,
    DATE_FORMAT(a.event_time, 'yyyy-MM-dd')                         AS recall_date
FROM alerts_t AS a
CROSS JOIN UNNEST(a.affected_lots) AS t(lot_number)
LEFT JOIN products_t AS p ON a.gtin = p.gtin
WHERE a.alert_type = 'RECALL'
  AND a.severity IN ('CLASS_I', 'CLASS_II')
  AND CARDINALITY(a.affected_lots) > 0
ORDER BY a.severity ASC, a.event_time DESC
LIMIT 30;


-- =============================================================================
-- SOLUTION 11  ⭐⭐⭐  OVER Aggregation — Price History with Ranking
-- =============================================================================
SELECT
    gtin,
    DATE_FORMAT(event_time, 'yyyy-MM-dd')       AS effective_date,
    new_public_price_chf,
    change_reason,
    -- Running average price for this GTIN up to and including this event
    ROUND(AVG(new_public_price_chf) OVER (
        PARTITION BY gtin
        ORDER BY event_time
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2)                                       AS running_avg_price,
    -- Rank by price level: highest price = rank 1
    RANK() OVER (
        PARTITION BY gtin
        ORDER BY new_public_price_chf DESC
    )                                           AS price_rank,
    -- Sequential update number for this GTIN (1 = first ever update)
    ROW_NUMBER() OVER (
        PARTITION BY gtin
        ORDER BY event_time ASC
    )                                           AS update_number
FROM prices_t
LIMIT 20;


-- =============================================================================
-- SOLUTION 12  ⭐⭐⭐  MATCH_RECOGNIZE — Shortage → Recall Pattern
-- =============================================================================
SELECT
    gtin,
    shortage_alert_id,
    recall_alert_id,
    shortage_headline,
    recall_headline,
    shortage_date,
    recall_date,
    days_between,
    'SHORTAGE_ESCALATED_TO_RECALL'      AS pattern_name
FROM alerts_t
MATCH_RECOGNIZE (
    PARTITION BY gtin
    ORDER BY event_time
    MEASURES
        S.alert_id                                              AS shortage_alert_id,
        R.alert_id                                              AS recall_alert_id,
        S.headline                                              AS shortage_headline,
        R.headline                                              AS recall_headline,
        DATE_FORMAT(S.event_time, 'yyyy-MM-dd')                 AS shortage_date,
        DATE_FORMAT(R.event_time, 'yyyy-MM-dd')                 AS recall_date,
        TIMESTAMPDIFF(DAY, S.event_time, R.event_time)         AS days_between
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO NEXT ROW
    PATTERN (S R)
    WITHIN INTERVAL '60' DAY
    DEFINE
        S AS S.alert_type = 'SHORTAGE',
        R AS R.alert_type = 'RECALL'
);
