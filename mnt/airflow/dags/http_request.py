import os
from dotenv import load_dotenv

from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json

load_dotenv()

API_KEY = os.getenv("PUBLIC_API_KEY")

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

session = Session()
session.headers.update(headers)

try:
    response = session.get(url, params=params)
    data = json.load(response.text)
    print(data)

except (ConnectionError, Timeout, TooManyRedirects) as e:
    print(e)
