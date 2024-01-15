# import libraries
from util import get_redshift_connection,\
execute_sql
import pandas as pd
import requests
import boto3
import psycopg2
from datetime import datetime
import csv
import json
import ast
from dotenv import dotenv_values
dotenv_values()
# Get credentials from environment variable file
config = dotenv_values('.env')
# Create a boto3 s3 client for bucket operations
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
#Function to pull the data from the rapid API
def dailyjob_data_extraction(country):
    url = config.get('URL')
    headers = ast.literal_eval(config.get('HEADERS'))
    querystring = ast.literal_eval(config.get('QUERYSTRING'))
    raw_job_data = []

    try:
        # Send request to Rapid API and return the response as a Json object
        response = requests.get(url, headers=headers, params=querystring, timeout=10).json()
        raw_job_data = response.get('data', [])
    except ConnectionError:
        print('Unable to fetch data via the URL endpoint')
    
    # Print unique country names from the API response
    unique_countries = set(job.get('job_country') for job in raw_job_data)
    print("Unique Countries in API Response:", unique_countries)
    # Using list comprehension to filter jobs based on the specified country
    filtered_jobs = [
        job for job in raw_job_data if (
            job.get('job_job_title') in ['Data engineer', 'Data analyst'] and
            job.get('job_country') == country
        )
    ]
    json_data = json.dumps(filtered_jobs, indent=2)  
    return json_data

# Function to transformed the data into csv format
def transform_data_to_csv(raw_data):
    transformed_data = []
    for job in raw_data:
        if isinstance(job, dict):
            employer_company_type = job.get('employer_company_type', '')
            transformed_job = {
                'employer_website': job.get('employer_website', ''),
                'job_id': job.get('job_id', ''),
                'job_employment_type': job.get('job_employment_type', ''),
                'job_title': job.get('job_title', ''),
                'job_apply_link': job.get('job_apply_link', ''),
                'job_description': job.get('job_description', ''),
                'job_city': job.get('job_city', ''),
                'job_country': job.get('job_country', ''),
                'job_posted_at_timestamp': datetime.fromtimestamp(job.get('job_posted_at_timestamp', 0)).strftime('%Y-%m-%d:%H:%M:%S'),
                'employer_company_type': employer_company_type
            }
            transformed_data.append(transformed_job)
    df = pd.DataFrame(transformed_data, columns=['employer_website', 'job_id', 'job_employment_type', 'job_title', 'job_apply_link', 'job_description', 'job_city', 'job_country', 'job_posted_at_timestamp', 'employer_company_type'])
    return df


#Function to load the transformed data to redshift
def load_to_redshift(bucket_name, folder_name, file_name, redshift_table_name):
    iam_role = config.get('IAM_ROLE')
    conn = get_redshift_connection()
    file_path = f's3://{bucket_name}/{folder_name}/{file_name}.csv'

    copy_query = f"""
        COPY {redshift_table_name}
        FROM '{file_path}'
        IAM_ROLE '{iam_role}'
        CSV
        DELIMITER ','
        QUOTE '"'
        ACCEPTINVCHARS
        TIMEFORMAT 'auto'
        IGNOREHEADER 1
    """
    execute_sql(copy_query, conn)
    print('Data successfully loaded to Redshift')
