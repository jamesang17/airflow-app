# airflow-app
 This is a simple project so I can learn how Airflow, Docker, and AWS (S3, DynamoDB) work together.
 
 Airflow handles the ingestion of the data.
 MySQL is be the database for Airflow keeping track of all runs.
 Docker containerizes Airflow and MySQL for scalability.
 AWS S3 is a staging location for the data before it gets imported into DynamoDB.
 DynamoDB is the main database for querying from the UI.

## Airflow

#### Dags
[Tweet Analyzer](https://github.com/jamesang17/airflow-app/blob/master/airflow/dags/tweet_analyzer.py) - pulls tweets around a certain topic, analyzes the text of the tweets for sentiment and writes the results to a db.

#### Custom Plugins
**[Twitter Plugin](https://github.com/jamesang17/airflow-app/tree/master/airflow/plugins/twitter_plugin)** has TwitterHook and Twitter to s3 operator to connect to twitter API via, pull tweets around a given topic and write the tweets out to S3.
 - Hooks
   - [TwitterHook](https://github.com/jamesang17/airflow-app/blob/master/airflow/plugins/twitter_plugin/hooks/twitter_hook.py)
 - Operators
   - [TweetsToS3Operator](https://github.com/jamesang17/airflow-app/blob/master/airflow/plugins/twitter_plugin/operators/tweets_to_s3_operator.py)

**[AWS Plugin](https://github.com/jamesang17/airflow-app/tree/master/airflow/plugins/aws_plugin)** has the operator to pull json data from S3 and write the data out to DynamoDB.
 - Operators
   - [S3ToDynamoDBOperator](https://github.com/jamesang17/airflow-app/blob/master/airflow/plugins/aws_plugin/operators/s3_to_dynamodb.py)

**[SODA Plugin](https://github.com/jamesang17/airflow-app/tree/master/airflow/plugins/soda_plugin)** has operator "SodaToS3Operator" to pull data for a certain dataset from the Socrata Open Data API and write it as a json file on S3.
 - Operators
   [SodaToS3Operator](https://github.com/jamesang17/airflow-app/blob/master/airflow/plugins/soda_plugin/operators/soda_to_s3_operator.py)
   
#### Scripts
Using the out of the box S3 Transform File Operator, there are 2 main scripts that were created to clean the tweets before uploading the data into DynamoDB. 
1. _[clean_tweets_pipeline.py](https://github.com/jamesang17/airflow-app/blob/master/airflow/scripts/etl/clean_tweets_pipeline.py)_ which extracts the desired fields from each tweepy SearchResultsObject, cleans the text of the tweet for sentiment analysis, and writes each tweet as a json object to a file.
2. _[sentiment_analysis.py](https://github.com/jamesang17/airflow-app/blob/master/airflow/scripts/nlp/sentiment_analysis.py)_, which runs a pretrained sentiment analysis model from [BlobText](https://textblob.readthedocs.io/en/dev/quickstart.html#sentiment-analysis) and generates a polarity score within [-1.0, 1.0]. Scores above 0 are considered to be positve, below 0 are considered to be negative and at or close to 0 are considered neutral.
3. _[summarize_results.py](https://github.com/jamesang17/airflow-app/blob/master/airflow/scripts/etl/summarize_results.py)_, which uses the output from the sentiment analysis script to get the average sentiment score from the data, the sentence with the highest and lowest sentiment score.


## Docker
Image: [jam717/airflow-app](https://hub.docker.com/repository/docker/jam717/airflow-app)

### Tech Stack
 - [Apache Airflow](https://airflow.apache.org/)
 - [AWS S3](https://aws.amazon.com/s3/)
 - [AWS DynamoDB](https://aws.amazon.com/dynamodb/)
 - [Docker](https://www.docker.com/)
 - [MySQL](https://www.mysql.com/)
