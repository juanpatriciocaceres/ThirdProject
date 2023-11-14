#!/bin/bash

# Initialize the database
airflow db init

# Create an admin user
airflow users create \
    --username admin \
    --firstname NOM_BRE \
    --lastname APELLIDO \
    --role Admin \
    --email email@gmail.com

# Start the web server, default port is 8080
airflow webserver -p 8080 &

# Start the scheduler
exec airflow scheduler
