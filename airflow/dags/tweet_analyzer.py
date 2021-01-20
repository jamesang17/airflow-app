from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.twitter_plugin import TweetsToS3Operator
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator
from airflow.operators.aws_plugin import S3ToDynamoDBOperator
from airflow.contrib.operators.s3_delete_objects_operator import S3DeleteObjectsOperator
import datetime,requests,collections,logging,json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    's3_bucket': 'j17devbucket',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily'
}

timestamp = '{{ ts_nodash }}'


with DAG(
    dag_id='tweet_analyzer', 
    default_args=default_args, 
    description='Pulls tweets about a given topic from twitter for analysis',
    #schedule_interval=timedelta(days=1),
    catchup=False
    ) as dag:

    tweets_to_s3 = TweetsToS3Operator(
        task_id='tweets_to_s3',
        topic='{{ dag_run.conf["topic"] }}',
        description='Writes tweets about a certain topic to S3',
        max_tweets=100,
        s3_key='tweet_data.' + timestamp
    )

    etl_tweets = S3FileTransformOperator(
        task_id='etl_tweets',
        description='cleans the tweet jsons pulled',
        source_s3_key='s3://j17devbucket/tweet_data.' + timestamp,
        dest_s3_key='s3://j17devbucket/cleaned_tweet_data.' + timestamp,
        replace=True,
        transform_script='scripts/etl/clean_tweets_pipeline.py'
    )

    get_sentiment = S3FileTransformOperator(
        task_id='get_sentiment',
        description='Get sentiment of tweets',
        source_s3_key='s3://j17devbucket/cleaned_tweet_data.' + timestamp,
        dest_s3_key='s3://j17devbucket/analyzed_tweet_data_' + timestamp + '.json',
        replace=True,
        transform_script='scripts/nlp/sentiment_analysis.py'
    )

    summarize_sentiment = S3FileTransformOperator(
        task_id='summarize_sentiment',
        description='Summarize sentiment of topic',
        source_s3_key='s3://j17devbucket/analyzed_tweet_data_' + timestamp + '.json',
        dest_s3_key='s3://j17devbucket/sentiment_results_' + timestamp + '.json',
        replace=True,
        transform_script='scripts/etl/summarize_results.py'
    )

    # tweets_to_dynamoDB = S3ToDynamoDBOperator(
    #     task_id='write_tweets_to_dynamoDB',
    #     description='Writes analzyed tweets to dynamoDB',
    #     table_name='jam717-tweets',
    #     table_keys=[
    #         "tweet_id","created_at","screen_name","text",
    #         "location","favorite_count","retweet_count","sentiment","topic"
    #         ],
    #     region_name='us-east-1',
    #     s3_key='s3://j17devbucket/analyzed_tweet_data_' + timestamp + '.json',
    #     json_key='tweets'
    # )

    sentiment_results_to_dynamoDB = S3ToDynamoDBOperator(
        task_id='write_sentiment_to_dynamoDB',
        description='Writes sentiment results to dynamoDB',
        table_name='sentiment-results',
        table_keys=["topic","timestamp","maxNegText","maxPosText","sentiment"],
        region_name='us-east-1',
        s3_key='s3://j17devbucket/sentiment_results_' + timestamp + '.json',
        json_key='results'
    )

    clean_up = S3DeleteObjectsOperator(
        task_id='clean_up_s3',
        description='Clean up files on s3',
        bucket='j17devbucket',
        keys=['tweet_data.' + timestamp,
           'cleaned_tweet_data.' + timestamp,
           'analyzed_tweet_data_' + timestamp + '.json',
           'sentiment_results_' + timestamp + '.json'
           ],
    )
    
    tweets_to_s3 >> etl_tweets >> get_sentiment >> summarize_sentiment >> sentiment_results_to_dynamoDB >> clean_up
    #tweets_to_s3 >> etl_tweets >> get_sentiment >> summarize_sentiment >> [tweets_to_dynamoDB,sentiment_results_to_dynamoDB] >> clean_up