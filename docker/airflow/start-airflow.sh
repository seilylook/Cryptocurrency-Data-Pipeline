#!/usr/bin/env bash

# Create the user airflow in the HDFS
hdfs dfs -mkdir -p    /user/airflow/
hdfs dfs -chmod g+w   /user/airflow

# Move th the AIRFLOW HOME directory
cd $AIRFLOW_HOME

# Export environment variables
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# Initialize the metadatabase
airflow db init

# Create User
airflow users create -e "admin@airflow.com" -f "airflow" -l "airflow" -p "airflow" -r "Admin" -u "airflow"

# Run the scheduler in background
airflow scheduler &> /dev/null &

# Run the web server in foreground(for docker logs)
exec airflow webserver