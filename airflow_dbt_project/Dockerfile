from apache/airflow:3.4.0

user root

run apt-get update && apt-get install -y gcc && apt-get clean

user airflow

copy requirements.txt .

run pip install --no-cache-dir -r requirements.txt