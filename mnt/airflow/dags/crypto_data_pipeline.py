from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from datetime import datetime, timedelta
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json

API_KEY = Variable.get("PUBLIC_API_KEY")

url = "https://sandbox-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
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
        response = session.get(url, params=params)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        data = response.json()  # Correctly parse JSON response
        print("----------")
        print(json.dumps(data, indent=2))  # Pretty print the JSON data
        print("----------")

    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)
    except Exception as e:
        print(f"Unexpected error: {e}")


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

    is_crypto_value_available >> get_crypto_data
