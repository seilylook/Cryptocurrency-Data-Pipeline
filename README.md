# Flow chart of the Cryptocurrency Data Pipeline

1. check availability of Cryptocurrency rates
2. check availability of the file having currencies to watch
3. download Cryptocurrency rates with python
4. save the Cryptocurrency rates in HDFS
5. create a hive table to store Cryptocurrency rates from the HDFS
6. process Cryptocurrency rates with Spark
7. send an email notificatoin
