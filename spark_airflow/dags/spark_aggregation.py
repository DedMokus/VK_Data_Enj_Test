from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import datetime
import logging


def files_extract(**kwargs):
        dt = datetime.datetime.strptime(kwargs["ds"], '%Y-%m-%d')

        dt_begin = dt - datetime.timedelta(days=7)

        files = [f"{datetime.datetime.strftime(dt_begin + datetime.timedelta(days=i), '%Y-%m-%d')}.csv" for i in range(7)]

        return files

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'spark_data_aggregate',
    default_args=default_args,
    description='DAG to aggregate user actions using Spark',
    schedule_interval='0 7 * * *',
    catchup=False,
)

extract_task = PythonOperator(
    task_id='files_extract_task',
    python_callable=files_extract,
    provide_context=True,
    dag=dag
)

# Задача для запуска Spark-программы
days_aggregate_task = SparkSubmitOperator(
    application="/app/spark_app/day_aggregation.py",
    name="spark_day_aggregation",
    conn_id="spark_default",
    application_args=["--execution_date","{{ ds }}"],  # Передаем дату исполнения как аргумент
    task_id="run_spark_aggregation",
    dag=dag,
)

week_aggregate_task = SparkSubmitOperator(
    application="/app/spark_app/week_aggregation.py",
    name="spark_week_aggregation",
    conn_id="spark_default",
    application_args=["--execution_date","{{ ds }}"],  
    task_id="run_spark_week_aggregation",
    dag=dag,
)

extract_task >> days_aggregate_task >> week_aggregate_task
