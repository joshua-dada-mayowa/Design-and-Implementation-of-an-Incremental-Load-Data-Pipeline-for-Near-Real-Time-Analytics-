-- Create Table
-- Prepare data
WITH product_data AS (
    SELECT
        r.product_id,
        p.product_name,
        COUNT(r.review) AS num_reviews
    FROM
        staging.fact_reviews AS r
        LEFT JOIN staging.dim_products AS p ON r.product_id = p.product_id
    GROUP BY
        r.product_id, p.product_name
    ORDER BY
        COUNT(r.review) DESC
    LIMIT 1
),
ordered_day_data AS (
    SELECT
        o.order_date AS most_ordered_day
    FROM
        staging.fact_orders AS o
        WHERE o.product_id = (SELECT product_id FROM product_data)
    GROUP BY
        o.order_date
    ORDER BY
        COUNT(1) DESC
    LIMIT 1
),
public_holiday_data AS (
    SELECT
        CASE WHEN d.working_day = 'false' THEN 0 ELSE 1 END AS is_public_holiday
    FROM
        staging.dim_dates AS d
        WHERE d.calendar_dt = (SELECT most_ordered_day FROM ordered_day_data)
),
total_review_points AS (
    SELECT
        SUM(review) AS tt_review
    FROM
        staging.fact_reviews AS r
        WHERE r.product_id = (SELECT product_id FROM product_data)
),
-- One star review
one_star_review AS(
SELECT COUNT(1) AS one_star
FROM staging.fact_reviews AS r
WHERE r.product_id = (SELECT p.product_id
					 FROM product_data AS p)
	AND r.review = 1)
	
,
-- Two_star_review
two_star_review AS (
SELECT COUNT(1) AS two_star
FROM staging.fact_reviews AS r
WHERE r.product_id = (SELECT p.product_id
					 FROM product_data AS p)
	AND r.review = 2)
,
-- Three_star_review 
three_star_review AS(
SELECT COUNT(1) AS three_star
FROM staging.fact_reviews AS r
WHERE r.product_id = (SELECT p.product_id
					 FROM product_data AS p)
	AND r.review = 3)
,
-- Four_star_review
four_star_review AS(
	SELECT COUNT(1) AS four_star
FROM staging.fact_reviews AS r
WHERE r.product_id = (SELECT p.product_id
					 FROM product_data AS p)
	AND r.review = 4)
,
-- Five star review
five_star_review AS(
	SELECT COUNT(1) AS five_star
FROM staging.fact_reviews AS r
WHERE r.product_id = (SELECT p.product_id
					 FROM product_data AS p)
	AND r.review = 5)
,
-- one star percentage
one_percentage AS (
SELECT ROUND(((SELECT one_star FROM one_star_review) * 100.0) / (SELECT num_reviews FROM product_data), 2) 
	AS one_pct)
,
-- Two star percentage
two_percentage AS(
SELECT ROUND(((SELECT two_star FROM two_star_review) * 100.0) / (SELECT num_reviews FROM product_data), 2) 
	AS two_pct)
,
-- three_star_percentage
three_percentage AS(
SELECT ROUND(((SELECT three_star FROM three_star_review) * 100.0) / (SELECT num_reviews FROM product_data), 2) 
	AS three_pct)
,
-- four star percentage
four_percentage AS (
SELECT ROUND(((SELECT four_star FROM four_star_review) * 100.0) / (SELECT num_reviews FROM product_data), 2) 
	AS four_pct)
,
-- Five star percentage
five_percentage AS(
SELECT ROUND(((SELECT five_star FROM five_star_review) * 100.0) / (SELECT num_reviews FROM product_data), 2) 
	AS five_pct)
,
-- Total shipments
tt_shipments AS (
SELECT COUNT(1) AS tt_shipments_count
FROM staging.fact_shipment_deliveries AS s
INNER JOIN staging.fact_orders AS o
	ON s.order_id = o.order_id
WHERE o.product_id = (SELECT p.product_id 
					 FROM product_data AS p))
,
-- Late shipments count
late_shipments AS (
SELECT COUNT(1) AS late_shipments_count
FROM staging.fact_shipment_deliveries AS s
INNER JOIN staging.fact_orders AS o
	ON s.order_id = o.order_id
WHERE o.product_id = (SELECT p.product_id 
					 FROM product_data AS p)
	AND (CAST(s.shipment_date AS DATE) - CAST(o.order_date AS DATE)) >= 6
	AND s.delivery_date IS NULL)
,
-- Late shipment percentage
late_shipment_percentage AS (SELECT ROUND(((SELECT late_shipments_count FROM late_shipments) * 100.0) / (SELECT tt_shipments_count FROM tt_shipments), 2) 
	AS late_shipment_percentage)
,
-- Early shipment count
early_shipments AS(
SELECT ((SELECT tt_shipments_count FROM tt_shipments) - (SELECT late_shipments_count FROM late_shipments)) 
AS early_shipments_count)
,
-- Early shipment percentage
early_shipment_percentage AS(
SELECT ROUND(((SELECT early_shipments_count FROM early_shipments) * 100.0) / (SELECT tt_shipments_count FROM tt_shipments), 2) 
	AS early_shipment_percentage)
,
-- Combine all CTEs into a single row
final_result AS (
    SELECT
        pd.product_name,
        odd.most_ordered_day,
        phd.is_public_holiday,
        trp.tt_review,
		osr.one_star,
		tsr.two_star,
		thr.three_star,
		fsr.four_star,
		fvr.five_star,
		osp.one_pct,
		tsp.two_pct,
		thp.three_pct,
		fsp.four_pct,
		fvp.five_pct,
		tts.tt_shipments_count,
		ls.late_shipments_count,
		es.early_shipments_count,
		lsp.late_shipment_percentage,
		esp.early_shipment_percentage
	
    FROM
        product_data AS pd
    JOIN
        ordered_day_data AS odd ON true
    JOIN
        public_holiday_data AS phd ON true
    JOIN
        total_review_points AS trp ON true
	JOIN
		one_star_review AS osr ON true
	JOIN
		two_star_review AS tsr ON true
	JOIN
		three_star_review AS thr ON true
	JOIN
		four_star_review AS fsr ON true
	JOIN
		five_star_review AS fvr ON true
	JOIN
		one_percentage AS osp ON true
	JOIN
		two_percentage AS tsp ON true
	JOIN 
		three_percentage AS thp ON true
	JOIN
		four_percentage AS fsp ON true
	JOIN 
		five_percentage AS fvp ON true
	JOIN
		tt_shipments AS tts ON true
	JOIN 
		late_shipments AS ls ON true
	JOIN
		early_shipments AS es ON true
	JOIN
		late_shipment_percentage AS lsp ON true
	JOIN
		early_shipment_percentage AS esp ON true
)

-- Insert into best_perfoming_table
INSERT INTO analytics.best_performing_product
SELECT
	CURRENT_DATE AS ingestion_date,
    product_name,
    most_ordered_day,
    CAST(is_public_holiday AS BOOL) AS is_public_holiday,
    tt_review AS tt_review_points,
	one_pct AS pct_one_star_review,
	two_pct AS pct_two_star_review,
	three_pct AS pct_three_star_review,
	four_pct AS pct_four_star_review,
	five_pct AS pct_five_star_review,
	early_shipment_percentage AS pct_early_shipments,
	late_shipment_percentage AS pct_late_shipments
	
FROM
    final_result;
