-- Create tables in the staging schema

CREATE SCHEMA IF NOT EXISTS staging;

-- Create table for orders
CREATE TABLE IF NOT EXISTS staging.orders (
    order_id BIGINT,
    customer_id BIGINT,
    order_date TEXT,
    product_id INT,
    unit_price NUMERIC,
    quantity INT,
    total_price NUMERIC
);

-- Create table for reviews
CREATE TABLE IF NOT EXISTS staging.reviews (
    review_id SERIAL PRIMARY KEY,
    product_idG BIGINT,
    review TEXT
);

-- Create table for shipments_deliveries
CREATE TABLE IF NOT EXISTS staging.shipment_deliveries (
    shipment_id BIGINT,
    order_id BIGINT,
    shipment_date VARCHAR(200),
    delivery_date VARCHAR(200)
);

