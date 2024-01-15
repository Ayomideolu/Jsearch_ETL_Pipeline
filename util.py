import boto3
import psycopg2
import json
import csv
import pandas as pd
from datetime import datetime
from dotenv import dotenv_values
dotenv_values()

# Get credentials from environment variable file
config = dotenv_values('.env')

# Create a boto3 s3 client and resource for bucket operations
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

# function to get Reshift data warehouse connection
def get_redshift_connection():
    user = config.get('USER')
    password = config.get('PASSWORD')
    host = config.get('HOST')
    database_name = config.get('DATABASE_NAME')
    port = config.get('PORT')
    conn = psycopg2.connect(f'postgresql://{user}:{password}@{host}:{port}/{database_name}')
    return conn



#Function to write the raw data to s3 bucket
def write_data_to_s3(extracted_data, bucket_name_raw, folder_name_raw, file_name_raw):
    s3 = boto3.client('s3')
    s3.put_object(Body=extracted_data,Bucket=bucket_name_raw, Key=f'{folder_name_raw}/{file_name_raw}.json', ContentType='application/json')
# write_data_to_s3(extracted_data, bucket_name_raw, folder_name_raw, file_name_raw)
#     print('All API data written to S3 bucket')


#Function to read the raw data from s3 bucket.
def read_data_from_s3(bucket_name_raw, folder_name_raw, file_name_raw):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name_raw, Key=f'{folder_name_raw}/{file_name_raw}.json')
    json_data = json.loads(response['Body'].read().decode('utf-8'))
    return json_data

# Function to write the transformed data to S3 bucket
def write_data_to_s3_csv(transformed_data, bucket_name, folder_name, file_name):
    csv_buffer = transformed_data.to_csv(index=False, encoding='utf-8', sep=',', quoting=csv.QUOTE_MINIMAL)
    s3 = boto3.client('s3')
    s3.put_object(Body=csv_buffer.encode('utf-8'), Bucket=bucket_name, Key=f'{folder_name}/{file_name}.csv', ContentType='text/csv')


#Function to generate the table schema in the redshift instance.
def generate_schema(table_name,columns):
    create_table_statement = f'CREATE TABLE IF NOT EXISTS {table_name} (\n'
    column_type_query = ''
    columns = ['employer_website','job_id','job_employment_type','job_title','job_apply_link','job_description','job_city','job_country','job_posted_at_timestamp','employer_company_type']
    for column in columns:
        last_column = columns[-1]

        if column == 'job_posted_at_timestamp':
            column_type_query += f'"{column}" TIMESTAMP,\n'
        elif column == 'job_description':
            column_type_query += f'"{column}" VARCHAR(10000),\n'  
        else:
            column_type_query += f'"{column}" VARCHAR(255),\n'
    column_type_query = column_type_query.rstrip(',\n')  
    column_type_query += '\n);'
    output_query = create_table_statement + column_type_query
    return output_query
#Function to execute the sql query to copy the transformed data to redshift
def execute_sql(table_schema, conn):
    try:
        cur = conn.cursor()
        cur.execute(table_schema)
        conn.commit()
    except Exception as e:
        print(f"Error executing SQL query: {e}")
        raise  # Reraise the exception after printing details
    finally:
        cur.close()

def list_files_in_folder(bucket_name, folder):
    bucket_list = s3_client.list_objects(Bucket = bucket_name, Prefix = folder) # List the objects in the bucket
    bucket_content_list = bucket_list.get('Contents')
    files_list = [file.get('Key') for file in bucket_content_list][1:]
    return files_list
        
def move_files_to_archived_folder(bucket_name, raw_data_folder, archived_data_folder):
    file_paths = list_files_in_folder(bucket_name, raw_data_folder)
    for file_path in file_paths:
        file_name = file_path.split('/')[-1]
        copy_source = {'Bucket': bucket_name, 'Key': file_path}
        # Copy files to processed folder
        s3_resource.meta.client.copy(copy_source, bucket_name, archived_data_folder + '/' + file_name)
        s3_resource.Object(bucket_name, file_path).delete()
    print("Files successfully moved to archived_data folder in S3")