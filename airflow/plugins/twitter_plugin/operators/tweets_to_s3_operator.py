from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from twitter_plugin.hooks.twitter_hook import TwitterHook
from tempfile import NamedTemporaryFile
from tweepy import TweepError
from tweepy import parsers
from tweepy import API
from datetime import date
import logging
import json

class TweetsToS3Operator(BaseOperator):
    """
    Twitter tweets to S3 Operator

    Queries the Twitter API and writes the resulting data to a file.

    :param topic:               The destination s3 connection id.
    :type topic:                string
    :param s3_conn_id:          The destination s3 connection id.
    :type s3_conn_id:           string
    :param s3_bucket:           The destination s3 bucket.
    :type s3_bucket:            string
    :param s3_key:              The destination s3 key.
    :type s3_key:               string
    :param twitter_script:      location of the executable transformation script
    :type twitter_script:       str
    """


    template_fields = ('topic',
                       's3_key',)

    @apply_defaults
    def __init__(self,
                 s3_bucket,
                 s3_key,
                 topic = '',
                 twitter_script=None,
                 s3_conn_id = 'aws_default',
                 max_tweets = 100,
                 *args, **kwargs):

        super(TweetsToS3Operator, self).__init__(*args, **kwargs)

        # Default - set to 100
        self.topic = topic
        self.twitter_script = twitter_script
        self.max_tweets = max_tweets
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def get_tweets(self, api, query):
        #tweets = [status for status in Cursor(api.search, q=topic).items(100)]
        # using iterative approach to save on memory usage instead of using 
        # tweepy.Cursor, which tends to consume more memory than expected.
        tweets = []
        last_id = -1
        while len(tweets) < self.max_tweets:
            count = self.max_tweets - len(tweets)
            try:
                new_tweets = api.search(q=query + " -filter:retweets", count=count, max_id=str(last_id - 1), tweet_mode='extended')
                if not new_tweets:
                    break
                tweets.extend(new_tweets['statuses'])
                last_id = new_tweets['statuses'][-1]['id']
            except TweepError as e:
                # TODO: handle exception thrown, e.g. wait and retry
                break
        return tweets


    def get_trends(self, api):
        # Returns the top 50 trending topics for a specific WOEID
        # nyc_trends = api.trends_place(2459115)
        trend_list = api.trends_place(1)[0]['trends']
        trends = []
        # remove trend objects that have tweet_volume: None
        for trend in trend_list:
            if trend['tweet_volume']:
                trends.append(trend)
        trends = sorted(trends, key=lambda trend: (trend['tweet_volume']),reverse=True)

        # get tweets for top 5 trends
        tweets = []
        for i in range(5):
            query = trends[i]['query']
            for result in self.get_tweets(api, query):
                result["topic"] = trends[i]['name']
                tweets.append(json.dumps(result, ensure_ascii=False))
        return tweets


    def execute(self, context):
        """
        Execute the operator.
        This will get all the data from twitter on given topic and write it to a file.
        """

        # Get Authentication 
        auth = TwitterHook().get_conn()

        api = API(auth,parser=parsers.JSONParser())

        # Open a name temporary file to store output file until S3 upload
        with NamedTemporaryFile("wb") as tmp:


            if self.topic:
                logging.info("Preparing to gather tweets about %s", self.topic)
                tweet_results = self.get_tweets(api, self.topic)
                tweet_results_list = []
                for result in tweet_results:
                    result["topic"] = self.topic
                    tweet_results_list.append(json.dumps(result, ensure_ascii=False))
            else:
                logging.info("Preparing to gather top 5 US trends")
                tweet_results_list = self.get_trends(api)

            # output the records from the query to a file
            # the list of records is stored under the "records" key
            logging.info("Writing tweet statuses to: {0}".format(tmp.name))

            # combine tweet jsons in to new line delimited string, where each line is a single json obj
            tweet_results = '\n'.join(tweet_results_list)
            tmp.write(tweet_results.encode("utf-8"))

            logging.info("Uploading tweet status file to S3")
            # Flush the temp file and upload temp file to S3
            tmp.flush()

            s3 = S3Hook(self.s3_conn_id)

            s3.load_file(
                filename=tmp.name,
                key=self.s3_key,
                bucket_name=self.s3_bucket,
                replace=True
            )

            #s3.connection.close()

            tmp.close()

        logging.info("Upload successful")
