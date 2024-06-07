# Flow chart of the Cryptocurrency Data Pipeline

1. Check availability of Cryptocurrency cost in CoinMarketCap
2. Download top 10th Cryptocurrency(ex. BTC, ETH) cost with python
4. Save the Cryptocurrency cost in HDFS
5. Create a hive table to store Cryptocurrency cost from the HDFS
6. Process Cryptocurrency cost with Spark
7. Send an email notificatoin
