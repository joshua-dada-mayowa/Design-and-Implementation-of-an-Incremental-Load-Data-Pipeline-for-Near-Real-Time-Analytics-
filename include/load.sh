#!/bin/bash

# Error handling function
handle_error() {
    echo "Error: $1"
    exit 1
}

# Check if psql command is available
command -v psql >/dev/null 2>&1 || handle_error "psql command not found. Please make sure PostgreSQL client is installed."

# Define variables for file paths
ORDERS_CSV="/usr/local/airflow/include/data/orders.csv"
REVIEWS_CSV="/usr/local/airflow/include/data/reviews.csv"
SHIPMENTS_DELIVERIES_CSV="/usr/local/airflow/include/data/shipments_deliveries.csv"

# Define PostgreSQL connection details (modify as needed)
PG_USER="${PG_USER:-postgres}"
PG_PASSWORD="${PG_PASSWORD:-postgres}"
PG_HOST="${PG_HOST:-75.119.135.61}"
PG_PORT="${PG_PORT:-5732}"
PG_DATABASE="${PG_DATABASE:-analytics_db}"

# Prompt for password if not provided
if [ -z "$PG_PASSWORD" ]; then
    read -s -p "Enter password for user $PG_USER: " PG_PASSWORD
    echo
fi

# Construct the SQL script
SQL_SCRIPT=$(cat <<EOF
\COPY staging.orders(order_id, customer_id, order_date, product_id, unit_price, quantity, total_price) FROM '$ORDERS_CSV' DELIMITER ',' CSV HEADER;
\COPY staging.reviews(product_id, review) FROM '$REVIEWS_CSV' DELIMITER ',' CSV HEADER;
\COPY staging.shipment_deliveries(shipment_id, order_id, shipment_date, delivery_date) FROM '$SHIPMENTS_DELIVERIES_CSV' DELIMITER ',' CSV HEADER;
EOF
)

# Execute the SQL script using psql
psql_command="psql -U '$PG_USER' -d '$PG_DATABASE' -h '$PG_HOST' -p '$PG_PORT' -w -c \"$SQL_SCRIPT\""

eval "$psql_command" || handle_error "Failed to execute SQL script."
