FROM apache/airflow:2.4.3
FROM apache-airflow[amazon]

RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt