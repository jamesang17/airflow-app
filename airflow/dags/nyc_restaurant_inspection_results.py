#from datetime import timedelta
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.soda_plugin import SodaToS3Operator
import datetime,requests,collections,logging,json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def transform_rest_data():
    print("TRANSFORMING RESTAURANT DATA")
    file_path = './staging/'

    return True

def write_to_dynamo_db():
    print("WRITING RESTAURANT DATA TO DATABASE")
    return True

dag = DAG(
    dag_id='restaurant_data',
    default_args=default_args,
    description='Pulls restaurant health inspection data and loads it into Amazon RDS',
    #schedule_interval=timedelta(days=1),
)

data_to_s3 = SodaToS3Operator(
    task_id='data_to_S3',
    default_args=default_args,
    description='Writes restaurant health inspection data from SODA to S3',
    dataset_url='https://data.cityofnewyork.us/resource/43nn-pn8j.json',
    soql=("$select=camis,dba,cuisine_description,grade&"
          "$offset=0&"
          "$where=(grade=\"A\" OR grade=\"B\" OR grade=\"C\") AND "
                    "record_date >= \"2020-03-17T00:00:00.000\"&$limit=2"),
    s3_conn_id='j17devbucketdata',
    s3_bucket='j17devbucket',
    s3_key='data_file',
	dag=dag
)

clean_rest_data_pipeline = PythonOperator(
    task_id='etl_jsons',
    default_args=default_args,
    description='cleans the jsons pulled',
    python_callable=transform_rest_data,
    dag=dag
)

write_to_database = PythonOperator(
    task_id='write_to_db',
    default_args=default_args,
    description='writes the json files into the db',
    python_callable=write_to_dynamo_db,
    dag=dag
)

data_to_s3 >> clean_rest_data_pipeline >> write_to_database
