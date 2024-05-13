CREATE TABLE IF NOT EXISTS analytics.best_performing_product(
	Ingestion_date DATE PRIMARY KEY NOT NULL,
	product_name VARCHAR(50) NOT NULL,
	most_ordered_day DATE NOT NULL,
	is_public_holiday BOOL NOT NULL,
	tt_review_points INT NOT NULL,
	pct_one_star_review FLOAT NOT NULL,
	pct_two_star_review FLOAT NOT NULL,
	pct_three_star_review FLOAT NOT NULL,
	pct_four_star_review FLOAT NOT NULL,
	pct_five_star_review FLOAT NOT NULL,
	pct_early_shipments FLOAT NOT NULL,
	pct_late_shipments FLOAT NOT NULL
);