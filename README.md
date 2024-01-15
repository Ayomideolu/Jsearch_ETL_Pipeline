# JobSearch_ETL_Pipeline

## Description:
This ETL (Extract, Transform, Load) pipeline is designed for extracting job search data from a Rapid API, specifically focusing on Data Engineer and Data Analyst positions. The goal is to transform the retrieved data into a standardized JSON format, store it in an Amazon S3 bucket, and then further process it by transforming it into CSV format. The final step involves loading the transformed data into an AWS Redshift data warehouse.

## Pre-requisite:
- Python 3.x installed
- AWS Redshift instance set up with the necessary IAM role
- Rapid API key for accessing job search data

## Environment Setup:
1. **Clone the repository:**
   ```bash
   git clone https://github.com/your_username/JobSearch-ETL-Pipeline.git
   cd JobSearch-ETL-Pipeline
## Install dependencies:
pip install -r requirements.txt


## Configuration Details (.env file):

### API credentials:
- `URL`: Rapid API endpoint for job search.
- `QUERYSTRING`: Query parameters for job search (e.g., country, job titles).
- `HEADERS`: Headers for authenticating with Rapid API.

### Redshift Credentials:
- `IAM_ROLE`: IAM Role ARN for Redshift data access.
- `USER`: Redshift username.
- `PASSWORD`: Redshift password.
- `HOST`: Redshift cluster endpoint.
- `DATABASE_NAME`: Redshift database name.
- `PORT`: Redshift cluster port (default is 5439).

## Running the program:
Execute the ETL pipeline by running the `main.py` script:

```bash
python main.py

## Output:
--Raw job data is stored in the specified S3 bucket and folder.
--Transformed data is stored in another S3 bucket and folder.
--Redshift table 'dailyjobsearch' is created with the necessary schema.
--Transformed data is loaded into the 'dailyjobsearch' table in Redshift.
## Additional Notes:
--The pipeline focuses on extracting Data Engineer and Data Analyst jobs from the Rapid API.
--The raw data is initially stored in S3, followed by transformation into a standardized CSV format.

