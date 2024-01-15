from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

from etl import (
    dailyjob_data_extraction,
    transform_data_to_csv,
    load_to_redshift,
)
from util import (
    get_redshift_connection,
    generate_schema,
    execute_sql,
    write_data_to_s3,
    read_data_from_s3,
    write_data_to_s3_csv,
    list_files_in_folder,
    move_files_to_archived_folder,
)

# Define default_args and DAG configuration
default_args = {
    'owner': 'Ayomide',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG (
    'job_data_dag',
    default_args=default_args,
    description='ETL process for daily job data from the API',
    schedule_interval= '0 8 * * *',  # Run every 24 hours
    catchup =False,
)

# Task 1: Extract data
def extract_data_func(**kwargs):
    extracted_data = dailyjob_data_extraction('US')
    write_data_to_s3(extracted_data, 'job-search-data1', 'raw_jobs_data', 'json_job_data')

extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data_func,
    provide_context=True,
    dag=dag,
)

# Task 2: Transform data
def transform_data_func(**kwargs):
    extracted_data = read_data_from_s3('job-search-data1', 'raw_jobs_data', 'json_job_data')
    transformed_data = transform_data_to_csv(extracted_data)
    write_data_to_s3_csv(transformed_data, 'job-search-data2', 'transformed_jobs_data', 'transformed_data')

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data_func,
    provide_context=True,
    dag=dag,
)

# Task 3: Load data to Redshift
def load_to_redshift_func(**kwargs):
    conn = get_redshift_connection()
    create_table_query = generate_schema('dailyjobsearch', [])  # Pass an empty list for now
    execute_sql(create_table_query, conn)
    load_to_redshift('job-search-data2', 'transformed_jobs_data', 'transformed_data', 'dailyjobsearch')

load_to_redshift_task = PythonOperator(
    task_id='load_to_redshift',
    python_callable=load_to_redshift_func,
    provide_context=True,
    dag=dag,
)

# Task 4: Move job data to archived folder
def move_files_func(**kwargs):
    move_files_to_archived_folder('job-search-data2', 'transformed_jobs_data', 'archived_data')

move_files_task = PythonOperator(
    task_id='move_files',
    python_callable=move_files_func,
    provide_context=True,
    dag=dag,
)
wait = BashOperator(
task_id = 'wait_some_minutes',
bash_command = 'sleep 60'
)

start = DummyOperator(
task_id = 'start')

end = DummyOperator(
task_id = 'end')



# Set task dependencies
start >> extract_data_task >> transform_data_task >> load_to_redshift_task >> wait >> move_files_task >> end


































# # Task 1: Extract data
# with dag:
#     start = DummyOperator(task_id='start')

#     extract_data_task = PythonOperator(
#         task_id='extract_data',
#         python_callable=dailyjob_data_extraction,
#         provide_context=True,
#     )

#     # Task 2: Transform data
#     transform_data_task = PythonOperator(
#         task_id='transform_data',
#         python_callable=transform_data_to_csv,
#         provide_context=True,
#     )

#     # Task 3: Load data to Redshift
#     load_to_redshift_task = PythonOperator(
#         task_id='load_to_redshift',
#         python_callable=load_to_redshift,
#         provide_context=True,
#     )

#     # Task 4: Move job data to archived folder
#     move_files_task = PythonOperator(
#         task_id='move_files',
#         python_callable = move_files_to_archived_folder,
#         provide_context=True,
#     )

#     wait = BashOperator(
#         task_id='wait_some_minutes',
#         bash_command='sleep 60',
#     )

#     end = DummyOperator(task_id='end')

#     # Set task dependencies
#     start >> extract_data_task >> transform_data_task >> load_to_redshift_task >> wait >> move_files_task >> end

