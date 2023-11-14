#!/bin/bash

# Initialize the database
airflow db init

# Create an admin user
airflow users create \
    --username admin \
    --firstname JUAN_PATRICIO \
    --lastname CACERES \
    --role Admin \
    --email juanpatriciocaceres@gmail.com

# Start the web server, default port is 8080
airflow webserver -p 8080 &

# Start the scheduler
exec airflow scheduler
