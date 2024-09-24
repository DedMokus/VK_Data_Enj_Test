from airflow.decorators import dag, task
import datetime
import sys
import pandas as pd
import os


@dag(
    dag_id="data_aggregate",
    description="Aggregation of weekly activity data",
    schedule="0 4 * * *",
    start_date=datetime.datetime(2024, 9, 20),
    catchup=False
)
def aggregate_data():

    @task()
    def files_extract(**kwargs):
        dt = datetime.datetime.strptime(kwargs["ds"], '%Y-%m-%d')

        dt_begin = dt - datetime.timedelta(days=7)

        files = [f"{datetime.datetime.strftime(dt_begin + datetime.timedelta(days=i), '%Y-%m-%d')}.csv" for i in range(7)]

        return files
    
    @task()
    def days_aggregate(files):
        days_files = []
        for file in files:
            file_path = os.path.join("/app/input", file)
            file_aggregated_path = os.path.join("/app/days_aggregate", file)

            if not os.path.exists(file_path):
                print(f"File {file} not found, skip.")
                continue

            if not os.path.exists(file_aggregated_path):

                day_data = pd.read_csv(file_path, names=["email", "type", "date"], sep=",")

                aggregated = day_data.groupby(["email", "type"]).size().unstack(fill_value=0)

                aggregated = aggregated.rename(columns={
                    "CREATE":"create_count",
                    "DELETE":"delete_count",
                    "UPDATE":"update_count",
                    "READ":"read_count"
                }).reindex(columns=["create_count", "read_count", "update_count", "delete_count"], fill_value=0)

                aggregated = aggregated.reset_index()

                aggregated.to_csv(file_aggregated_path, index=False)
            
            days_files.append(file_aggregated_path)
        
        return days_files
            

    @task()
    def week_aggregate(files, **kwargs):

        dt = datetime.datetime.strptime(kwargs["ds"], '%Y-%m-%d')
        dt_file_name = f"{dt.strftime('%Y-%m-%d')}.csv"
        print(dt_file_name)

        agregated_dfs = [pd.read_csv(file) for file in files]

        combined_csv = pd.concat(agregated_dfs)

        fin = combined_csv.groupby("email", as_index=False).sum()

        fin.to_csv(os.path.join("/app/output", dt_file_name), index=False)

    files_paths = files_extract()
    days_aggregated_paths = days_aggregate(files_paths)
    week_aggregate(days_aggregated_paths)

    return None

aggregate_data()