from airflow.utils.decorators import apply_defaults
from typing import List
from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_dynamodb_hook import AwsDynamoDBHook
from airflow.hooks.S3_hook import S3Hook
from decimal import Decimal
import boto3
import json
import logging


class S3ToDynamoDBOperator(BaseOperator):
    """
    Uploads json data from S3 to a DynamoDB table.

    :param table_name:              Dynamodb table to replicate data to
    :type table_name:               str
    :param table_keys:              partition key and sort key
    :type table_keys:               list
    :param aws_conn_id:             aws connection
    :type aws_conn_id:              str
    :param region_name:             aws region name (example: us-east-1)
    :type region_name:              str
    :param s3_key:                  s3 object key
    :type s3_key:                   str
    :param json_key:                Key for json list
    :type json_key:                 str

    """
    template_fields = ("table_name",
                       "json_key",
                       "s3_key"
                       )

    @apply_defaults
    def __init__(self,
                 table_name,
                 table_keys,
                 region_name,
                 s3_key,
                 json_key,
                 aws_conn_id = 'aws_default',
                 *args, **kwargs):
        
        super().__init__(*args, **kwargs)
        
        self.table_name = table_name
        self.table_keys = table_keys
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.s3_key = s3_key
        self.json_key = json_key

    def _convert_float_to_decimal(self, json_list):
        for json_obj in json_list:
            val = str(json_obj["sentiment"])
            json_obj["sentiment"] = Decimal(val)
        return json_list

    def execute(self, context):
        s3 = S3Hook(aws_conn_id=self.aws_conn_id)

        dynamodb = AwsDynamoDBHook(aws_conn_id=self.aws_conn_id,
                                   table_name=self.table_name,
                                   table_keys=self.table_keys,
                                   region_name=self.region_name) 

        if not s3.check_for_key(self.s3_key):
            raise AirflowException(
                "The source key {0} does not exist".format(self.s3_key))
        
        s3_key_object = s3.get_key(self.s3_key)
        s3_key_json = json.loads(s3_key_object.get()['Body'].read().decode('utf-8'))
        json_list = s3_key_json[self.json_key]


        json_list = self._convert_float_to_decimal(json_list)
        
        logging.info('Inserting rows into dynamodb table %s', self.table_name)
        dynamodb.write_batch_data(json_list)
        logging.info('Finished inserting %d rows into dynamodb table %s', len(json_list), self.table_name)
