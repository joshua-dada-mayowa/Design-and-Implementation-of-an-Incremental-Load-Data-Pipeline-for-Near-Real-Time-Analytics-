
from pendulum import datetime
import logging
import os
from astro import sql as aql
from airflow.decorators import dag, task,task_group
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import boto3
from botocore import UNSIGNED
import pandas as pd
from sqlalchemy import create_engine
from botocore.client import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

POSTGRES_CONN_ID = "postgres_default"

# AWS S3 bucket and file paths
BUCKET_NAME = "d2b-internal-assessment-bucket"
ORDERS_DATA_PATH = "orders_data/orders.csv"
REVIEWS_DATA_PATH = "orders_data/reviews.csv"


# shipments_data_path = "orders_data/shipments_deliveries.csv"
postgres_schema = Variable.get("POSTGRES_SCHEMA", default_var=None)




@dag(start_date=datetime(2024, 3, 21), schedule="@daily", template_searchpath="/usr/local/airflow/include/sql", catchup=False)
def full_load_s3_to_postgres():

    @task
    def download_from_s3():
        try:
            bucket_name=BUCKET_NAME
            files_to_download=[ORDERS_DATA_PATH,REVIEWS_DATA_PATH]
            # Initialize S3 client
            s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

            # Check if all files exist in the bucket
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix="orders_data")
            object_keys = [obj['Key'] for obj in response.get('Contents', [])]

            if all(file_key in object_keys for file_key in files_to_download):
                # Download each file
                s3.download_file(bucket_name, "orders_data/orders.csv", "./include/data/orders.csv")
                s3.download_file(bucket_name, "orders_data/shipment_deliveries.csv", "./include/data/shipment_deliveries.csv")
                s3.download_file(bucket_name, "orders_data/reviews.csv", "./include/data/reviews.csv")
                logging.info("All files downloaded from S3 successfully.")
            else:
                logging.error("Not all required files exist in the S3 bucket.")
        except Exception as e:
            logging.error(f"Error during S3 download: {str(e)}")

    download=download_from_s3()

    def read_csv(file_path, data_types, date_columns=None):
        """
        Read data from a CSV file into a pandas DataFrame.

        Args:
            file_path (str): Path to the CSV file.
            data_types (dict): Dictionary specifying data types for columns.
            date_columns (list, optional): List of column names to parse as dates. Defaults to None.

        Returns:
            pd.DataFrame: DataFrame containing the data from the CSV file.
        """
        return pd.read_csv(file_path, dtype=data_types, parse_dates=date_columns)
    
    def create_postgres_connection():
        """
        Create a PostgreSQL connection using environmental variables.

        Returns:
            sqlalchemy.engine.base.Engine: PostgreSQL engine.
        """
        db_name = Variable.get("DB_NAME", default_var=None)
        db_user = Variable.get("DB_USER", default_var=None)
        db_password = Variable.get("DB_PASSWORD", default_var=None)
        db_host = Variable.get("DB_HOST", default_var=None)
        db_port = 5732

        # Replace with your actual environmental variables
        db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
        engine = create_engine(db_url)
        engine.connect()
        return engine

    def create_and_load_table(engine, df, table_name, schema=postgres_schema, if_exists='replace', index=False):
        """
        Create a table in PostgreSQL and load data into it.

        Args:
            engine (sqlalchemy.engine.base.Engine): PostgreSQL engine.
            df (pd.DataFrame): DataFrame containing data to be loaded.
            table_name (str): Name of the table.
            schema (str, optional): Schema name. Defaults to 'public'.
            if_exists (str, optional): Behavior if the table already exists ('replace', 'append', or 'fail'). Defaults to 'replace'.
            index (bool, optional): Whether to include the DataFrame index as a column. Defaults to False.
        """
        df.head(n=0).to_sql(name=table_name, con=engine, schema=schema, if_exists=if_exists, index=index)
        df.to_sql(name=table_name, con=engine, schema=schema, if_exists='append', index=index)


    @task
    def loader():
    # Define data types for orders, reviews, and shipments
        try:
            orders_types = {
                'order_id': int,
                'customer_id': int,
                'product_id': int,
                'unit_price': float,
                'quantity': int,
                'total_price': float
            }

            reviews_types = {
                'review': int,
                'product_id': int
            }

            shipments_types = {
                'shipment_id': int,
                'order_id': int
            }

            # Read data from CSV files
            fact_orders = read_csv('./include/data/orders.csv', orders_types, date_columns=['order_date'])
            fact_reviews = read_csv('./include/data/reviews.csv', reviews_types)
            fact_shipment_deliveries = read_csv('./include/data/shipment_deliveries.csv', shipments_types, date_columns=['shipment_date', 'delivery_date'])

            engine = create_postgres_connection()

            # Create and load fact_orders table
            create_and_load_table(engine, fact_orders, 'fact_orders')
            # Create and load fact_reviews table
            create_and_load_table(engine, fact_reviews, 'fact_reviews', index=False)
            create_and_load_table(engine, fact_shipment_deliveries, 'fact_shipment_deliveries', index=False)
            logging.info("All files loaded to postgres successfully.")
        except Exception as e:
            logging.error(f"Error during load to postgres: {str(e)}")
    S3_to_postgres_loader=loader()




    @task_group()
    def create_analytics_table():
        create_best_performing_product= SQLExecuteQueryOperator(
                    task_id="create_best_performing_product",
                    conn_id=POSTGRES_CONN_ID,
                    retry_on_failure="True",
                    sql="create_best_performing_product.sql",
                )
        agg_public_holiday=SQLExecuteQueryOperator(
                    task_id="create_agg_public_holiday",
                    conn_id=POSTGRES_CONN_ID,
                    retry_on_failure="True",
                    sql="create_agg_public_holiday.sql",
                )        
        agg_shipments=SQLExecuteQueryOperator(
                    task_id="create_agg_shipments",
                    conn_id=POSTGRES_CONN_ID,
                    retry_on_failure="True",
                    sql="create_agg_shipments.sql",
                )
    create_analytics_tables=create_analytics_table()
        


    @task_group()
    def insert_analytics_table():
        insert_into_agg_shipments=  SQLExecuteQueryOperator(
                    task_id="insert_into_agg_shipments",
                    conn_id=POSTGRES_CONN_ID,
                    sql="insert_into_agg_shipments.sql",
                )
        insert_into_agg_public_holiday= SQLExecuteQueryOperator(    
                    task_id="insert_into_agg_public_holiday",
                    conn_id=POSTGRES_CONN_ID,
                    sql="insert_into_agg_public_holiday.sql",
                )
        insert_into_best_performing_product= SQLExecuteQueryOperator(    
                    task_id="insert_into_best_performing_product",
                    conn_id=POSTGRES_CONN_ID,
                    sql="insert_into_best_performing_product.sql",
                )
        
    insert_analytics_tables=insert_analytics_table()

    

    download >> S3_to_postgres_loader >> create_analytics_tables >> insert_analytics_tables



full_load_s3_to_postgres()