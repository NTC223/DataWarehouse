FROM apache/airflow:3.1.7-python3.10

COPY requirements.txt /requirements.txt

RUN pip install --upgrade pip
RUN pip install  -r /requirements.txt