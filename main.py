import subprocess
from datetime import datetime
from etl import (
    dailyjob_data_extraction,
    transform_data_to_csv,
    load_to_redshift
)
from util import get_redshift_connection, generate_schema, execute_sql, write_data_to_s3, \
    read_data_from_s3, write_data_to_s3_csv, list_files_in_folder, move_files_to_archived_folder

conn = get_redshift_connection()


def trigger_airflow_dag(dag_id):
    command = ["airflow", "trigger_dag", dag_id]
    subprocess.run(command)


def main():
    bucket_name_raw = 'job-search-data1'
    folder_name_raw = 'raw_jobs_data'
    file_name_raw = 'json_job_data'
    bucket_name_transformed = 'job-search-data2'
    folder_name_transformed = 'transformed_jobs_data'
    file_name_transformed = 'transformed_data'
    archived_data_transformed = 'archived_data'
    redshift_table_name = 'dailyjobsearch'
    airflow_dag_id = 'job_data_dag'  

    # Step 1: Extract data
    extracted_data = dailyjob_data_extraction('US')
    print(extracted_data)
    print('All API job_data pulled successfully in json format')

    # Step 2: Write raw data to S3
    write_data_to_s3(extracted_data, bucket_name_raw, folder_name_raw, file_name_raw)
    print('All API data written to S3 bucket')

    # Step 3: Read raw data from S3
    extracted_data = read_data_from_s3(bucket_name_raw, folder_name_raw, file_name_raw)
    print('raw_job_json data successfully read from s3 bucket job-search-data1')

    # Step 4: Transform data
    transformed_data = transform_data_to_csv(extracted_data)
    print('json_raw_data successfully transformed into csv format, relevant columns are kept')

    # Step 5: Write transformed data to another S3 bucket
    write_data_to_s3_csv(transformed_data, bucket_name_transformed, folder_name_transformed, file_name_transformed)
    print('transformed_data successfully written into s3 bucket job-search-data2')

    # Step 6: Load data to Redshift
    create_table_query = generate_schema(redshift_table_name, transformed_data)
    execute_sql(create_table_query, conn)
    print('Schema/Table created in Redshift')
    load_to_redshift(bucket_name_transformed, folder_name_transformed, file_name_transformed, redshift_table_name)
    # Step 7: Move job data to archived folder
    move_files_to_archived_folder(bucket_name_transformed, folder_name_transformed, archived_data_transformed)
    # Trigger Airflow DAG run
    trigger_airflow_dag(airflow_dag_id)


if __name__ == "__main__":
    main()
