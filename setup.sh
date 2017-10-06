#!/bin/sh
conda env list | grep -s airflow || conda create --name airflow
source activate airflow
# https://issues.apache.org/jira/browse/AIRFLOW-1102
conda install pandas
pip install airflow
pip uninstall -y gunicorn
pip install gunicorn==19.3.0
# airflow initdb
