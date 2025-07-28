from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from datetime import datetime, timedelta
import pandas as pd
import shutil
import glob
from pathlib import Path

# Constants
BUCKET_NAME = "sales-data-2016-2018"
EXPECTED_COLUMNS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def get_today_file_name(**kwargs):
    today_str = datetime.now().strftime("%Y%m%d")
    kwargs["ti"].xcom_push(key="file_date", value=today_str)


def validate_orders_file(**kwargs):
    ti = kwargs["ti"]
    file_date = ti.xcom_pull(task_ids="get_today_file_date", key="file_date")
    key = f"orders/orders_{file_date}.csv"

    # Download to a temp directory
    download_dir = "/tmp"
    try:
        hook = S3Hook(aws_conn_id="aws_conn")
        hook.download_file(
            key=key,
            bucket_name=BUCKET_NAME,
            local_path=download_dir
        )
        print(f"✅ File downloaded to {download_dir}")
    except Exception as e:
        raise RuntimeError(f"❌ Failed to download file: {e}")

    # Find the downloaded file and move it to the expected filename
    try:
        tmp_file = glob.glob("/tmp/airflow_tmp_*")[0]
        final_path = f"/tmp/orders_{file_date}.csv"
        shutil.move(tmp_file, final_path)
    except Exception as e:
        raise RuntimeError(f"❌ Failed to move temp file: {e}")


    try:
        if not Path(final_path).exists():
            raise FileNotFoundError(f"{final_path} does not exist after move!")
        # check columns
        df = pd.read_csv(final_path, nrows=10)
        print("✅ CSV preview:")
        print(df.head())
        if list(df.columns) != EXPECTED_COLUMNS:
            raise ValueError(f"❌ Column mismatch! Expected: {EXPECTED_COLUMNS}, Got: {list(df.columns)}")
        print("✅ Column names match expected schema.")

        # Check missing values in required fields
        required_columns = ["order_id", "customer_id"]
        missing = df[required_columns].isnull().sum()
        if missing.any():
            raise ValueError(f"❌ Missing values found:\n{missing[missing > 0]}")
        print("✅ No missing values in required columns.")

        # Check uniqueness of order_id
        if df["order_id"].duplicated().any():
            duplicates = df[df["order_id"].duplicated(keep=False)]
            raise ValueError(f"❌ Duplicate order_id values found:\n{duplicates}")
        print("✅ All order_id values are unique.")

        #print number of rows in the actual file
        csv_path = f"/tmp/orders_{file_date}.csv"
        df = pd.read_csv(csv_path)
        print(f"✅ Number of rows in the actual file: {len(df)}")

    except Exception as e:
        raise RuntimeError(f"❌ Validation failed: {e}")

def log_row_count(**context):
    result = context["ti"].xcom_pull(task_ids="count_loaded_rows_in_snowflake")
    print(f"🔍 Pulled from XCom: {result}")

    # Handle list of dicts like [{'COUNT(*)': 329}]
    if isinstance(result, list) and isinstance(result[0], dict):
        row_count = list(result[0].values())[0]  # or result[0]["COUNT(*)"]
    else:
        raise ValueError(f"Unexpected XCom result format: {result}")

    print(f"✅ Loaded {row_count} rows into Snowflake.")

with DAG(
    dag_id="data_validation_load",
    default_args=default_args,
    description="Load today's CSVs from S3 to Snowflake",
    start_date=datetime(2025, 7, 1),
    schedule_interval=None,
    catchup=False,
    tags=["orders", "products", "snowflake"],
) as dag:

    get_file_date = PythonOperator(
        task_id="get_today_file_date",
        python_callable=get_today_file_name,
    )

    validate_orders = PythonOperator(
        task_id="validate_orders_file",
        python_callable=validate_orders_file,
    )

    copy_orders = SnowflakeOperator(
        task_id="copy_orders_from_s3",
        sql="""
            COPY INTO orders
            FROM (
            SELECT $1, $2, $3, $4, $5, $6, $7, $8, CURRENT_DATE
            FROM @aws_stage/orders/orders_{{ ti.xcom_pull(task_ids='get_today_file_date', key='file_date') }}.csv)
            FILE_FORMAT = (FORMAT_NAME = 'sales_data_format')
            ON_ERROR = 'CONTINUE';
        """,
        snowflake_conn_id="snowflake_conn",
        warehouse="COMPUTE_WH",
        database="SALES_DATA",
        schema="SALES_DATA_SCHEMA",
    )

    count_loaded_rows = SnowflakeOperator(
    task_id="count_loaded_rows_in_snowflake",
    sql="SELECT COUNT(*) FROM orders WHERE load_date = CURRENT_DATE;",
    snowflake_conn_id="snowflake_conn",
    warehouse="COMPUTE_WH",
    database="SALES_DATA",
    schema="SALES_DATA_SCHEMA",
    do_xcom_push=True,
    )


    log_loaded_count = PythonOperator(
    task_id="log_loaded_row_count",
    python_callable=log_row_count,
    )



    get_file_date >> validate_orders >> copy_orders >> count_loaded_rows >> log_loaded_count
