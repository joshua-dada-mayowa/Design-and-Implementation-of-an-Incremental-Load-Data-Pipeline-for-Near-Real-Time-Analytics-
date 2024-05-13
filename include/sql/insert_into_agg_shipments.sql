-- Transform and insert data into table
INSERT INTO analytics.agg_shipments (ingestion_date, late_shipments, undelivered_shipments)
SELECT
    CURRENT_DATE AS ingestion_date,
    COUNT(1) FILTER (WHERE s.delivery_date IS NULL AND s.shipment_date IS NOT NULL AND (CAST(s.shipment_date AS DATE) - CAST(o.order_date AS DATE)) >= 6) AS late_shipments,
    COUNT(1) FILTER (WHERE s.delivery_date IS NULL AND s.shipment_date IS NULL AND ('2022-09-05' - CAST(o.order_date AS DATE)) = 15) AS undelivered_shipments
FROM
    staging.fact_shipment_deliveries AS s
    JOIN staging.fact_orders AS o ON s.order_id = o.order_id
GROUP BY CURRENT_DATE
ON CONFLICT (ingestion_date) DO UPDATE
    SET late_shipments = EXCLUDED.late_shipments,
        undelivered_shipments = EXCLUDED.undelivered_shipments;
