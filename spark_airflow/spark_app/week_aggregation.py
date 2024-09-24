from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum
import os
from datetime import datetime
import argparse

def week_aggregate():
    spark = SparkSession.builder.appName("WeekAggregate").getOrCreate()

    parser = argparse.ArgumentParser()
    parser.add_argument('--execution_date', required=True, help='Execution date passed from Airflow')

    # Parse the arguments
    args = parser.parse_args()

    # Use the execution date
    execution_date = args.execution_date

    dt = datetime.datetime.strptime(execution_date, '%Y-%m-%d')

    dt_begin = dt - datetime.timedelta(days=7)

    files = [f"{os.path.join('/app/days_aggregate', datetime.datetime.strftime(dt_begin + datetime.timedelta(days=i), '%Y-%m-%d'))}.csv" for i in range(7)]

    dt_file_name = f"{dt.strftime('%Y-%m-%d')}.csv"

    # Чтение всех файлов с помощью PySpark
    aggregated_dfs = [spark.read.csv(file, header=True, inferSchema=True) for file in files]

    # Объединение всех DataFrame
    combined_df = aggregated_dfs[0]
    for df in aggregated_dfs[1:]:
        combined_df = combined_df.union(df)

    # Группировка по email и агрегация по действиям
    final_df = combined_df.groupBy("email").agg(
        spark_sum("create_count").alias("create_count"),
        spark_sum("read_count").alias("read_count"),
        spark_sum("update_count").alias("update_count"),
        spark_sum("delete_count").alias("delete_count")
    )

    # Сохранение результирующего файла
    output_path = os.path.join("/app/output", dt_file_name)
    final_df.coalesce(1).write.csv(output_path, header=True)

    return output_path
