from pendulum import datetime
import pandas as pd
from airflow.decorators import dag,task
from astro.files import File
from astro import sql as aql
from astro.sql.table import Table

POSTGRES_CONN_ID = "postgres_default"
bucket_name = "d2b-internal-assessment-bucket"
orders_data_path = "orders_data/orders.csv"
reviews_data_path = "orders_data/reviews.csv"
shipments_data_path = "orders_data/shipments_deliveries.csv"


@dag(start_date=datetime(2024, 3, 23), schedule="@daily", template_searchpath="/usr/local/airflow/include/sql",catchup=False)
def elt_s3_to_postgres():

    order_csv = aql.load_file(
        task_id="load_order_csv",
        input_file=File(path= "./include/data/orders.csv"),
        output_table=Table(name="orders", conn_id=POSTGRES_CONN_ID),
    )
    reviews_csv = aql.load_file(
        task_id="load_reviews_csv",
        input_file=File(path= "./include/data/reviews.csv"),
        output_table=Table(name="reviews",conn_id=POSTGRES_CONN_ID),

    )

elt_s3_to_postgres()