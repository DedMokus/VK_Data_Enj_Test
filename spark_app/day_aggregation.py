from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
import os
import datetime
import sys
import argparse

def days_aggregate():
    spark = SparkSession.builder.appName("DaysAggregate").getOrCreate()

    # Create an argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument('--execution_date', required=True, help='Execution date passed from Airflow')

    # Parse the arguments
    args = parser.parse_args()

    # Use the execution date
    execution_date = args.execution_date

    dt = datetime.datetime.strptime(execution_date, '%Y-%m-%d')

    dt_begin = dt - datetime.timedelta(days=7)

    files = [f"{datetime.datetime.strftime(dt_begin + datetime.timedelta(days=i), '%Y-%m-%d')}.csv" for i in range(7)]
    
    days_files = []
    
    for file in files:
        file_path = os.path.join("/app/input", file)
        file_aggregated_path = os.path.join("/app/days_aggregate", file)

        if not os.path.exists(file_path):
            print(f"File {file} not found, skipping.")
            continue

        if not os.path.exists(file_aggregated_path):
            # Чтение CSV файла в Spark DataFrame
            day_data = spark.read.csv(file_path, header=False)
            day_data = day_data.withColumnRenamed("_c0", "email").withColumnRenamed("_c1", "type").withColumnRenamed("_c2", "date")

            # Агрегация по типу действий
            aggregated = day_data.groupBy("email").agg(
                count(when(col("type") == "CREATE", True)).alias("create_count"),
                count(when(col("type") == "READ", True)).alias("read_count"),
                count(when(col("type") == "UPDATE", True)).alias("update_count"),
                count(when(col("type") == "DELETE", True)).alias("delete_count")
            )

            # Сохранение агрегированных данных
            aggregated.coalesce(1).write.csv(file_aggregated_path, header=True)
            
        days_files.append(file_aggregated_path)
    
    return days_files
