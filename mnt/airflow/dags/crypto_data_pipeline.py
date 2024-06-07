from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator


from datetime import datetime, timedelta
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import os

API_KEY = Variable.get("PUBLIC_API_KEY")
CRYPTO_LIST = [
    "BTC",
    "ETH",
    "USDT",
    "BNB",
    "SOL",
    "USDC",
    "XRP",
    "DOGE",
    "TON",
    "ADA",
]
OUTPUT_DIR = os.path.join(os.environ["AIRFLOW_HOME"], "dags", "files")
URL = "https://sandbox-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest?symbol="

params = {
    "start": "1",
    "limit": "5000",
    "convert": "USD",
}
headers = {
    "Accepts": "application/json",
    "X-CMC_PRO_API_KEY": API_KEY,
}

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _get_crypto_data():
    session = Session()
    session.headers.update(headers)
    try:
        for crypto in CRYPTO_LIST:
            response = session.get(URL + crypto)
            response.raise_for_status()
            data = response.json()

            filename = f"{crypto}_data.json"
            filepath = os.path.join(OUTPUT_DIR, filename)

            with open(filepath, "w") as f:
                json.dump(data, f, indent=2)

            print(f"Data for {crypto} saved to {filepath}")

    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)
    except Exception as e:
        print(f"Unexpected error: {e}")


def _generate_hdfs_commands():
    files = os.listdir(OUTPUT_DIR)
    # print(files)

    commands = []
    for filename in files:
        if filename.endswith(".json"):
            commands.append(
                f"hdfs dfs -put -f {os.path.join(OUTPUT_DIR, filename)} /crypto"
            )

    return commands


def _save_files_to_hdfs():
    commands = _generate_hdfs_commands()
    for command in commands:
        os.system(command)
        print(f"Executed: {command}")


with DAG(
    "crypto_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag:
    is_crypto_value_available = SimpleHttpOperator(
        task_id="is_crypto_value_available",
        method="GET",
        http_conn_id="crypto_api",
        endpoint="v1/cryptocurrency/listings/latest",
        data=params,
        headers=headers,
        do_xcom_push=True,  # Enable XCom push to share data between tasks
    )

    get_crypto_data = PythonOperator(
        task_id="get_crypto_data",
        python_callable=_get_crypto_data,
    )

    make_directory_to_hdfs = BashOperator(
        task_id="make_directory_to_hdfs",
        bash_command="hdfs dfs -mkdir -p /crypto",
    )

    save_files_to_hdfs = PythonOperator(
        task_id="save_files_to_hdfs",
        python_callable=_save_files_to_hdfs,
    )

    create_crypto_data_table = HiveOperator(
        task_id="create_crypto_data_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS crypto_data(
                symbol STRING,
                id BIGINT,
                price DOUBLE,
                volume_24h DOUBLE,
                volume_change_24h DOUBLE,
                percent_change_1h DOUBLE,
                percent_change_24h DOUBLE,
                percent_change_7d DOUBLE,
                percent_change_30d DOUBLE
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """,
    )

    (
        is_crypto_value_available
        >> get_crypto_data
        >> make_directory_to_hdfs
        >> save_files_to_hdfs
        >> create_crypto_data_table
    )
