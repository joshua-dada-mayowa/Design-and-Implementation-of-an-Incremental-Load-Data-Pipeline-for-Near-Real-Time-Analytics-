-- Create the table enforcing the constraints
CREATE TABLE IF NOT EXISTS analytics.agg_shipments(
	ingestion_date DATE PRIMARY KEY NOT NULL,
	tt_late_shipments INT NOT NULL,
	tt_undelivered_items INT NOT NULL
);
