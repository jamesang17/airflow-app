from tempfile import NamedTemporaryFile
import logging
import json
import requests

from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook

class SodaToS3Operator(BaseOperator):
    """
    Socrata Open Data API dataset to S3 Operator

    Makes a query against SODA dataset and write the resulting data to a file.

    :param dataset_url:         The endpoint of the dataset json
    :type dataset_url:          string
    :param soql:                SODA SOQL Query String used to query Bulk API
    :type soql:                 string
    :param s3_conn_id:          The destination s3 connection id.
    :type s3_conn_id:           string
    :param s3_bucket:           The destination s3 bucket.
    :type s3_bucket:            string
    :param s3_key:              The destination s3 key.
    :type s3_key:               string
    """

    template_fields = ("dataset_url",
                       "s3_key",
                       "soql")

    @apply_defaults
    def __init__(self,
                 dataset_url,
                 soql,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 *args,
                 **kwargs):

        super(SodaToS3Operator, self).__init__(*args, **kwargs)

        self.dataset_url = dataset_url
        self.soql = soql
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def run_query(self, query, dataset_url):
        if not query:
            raise ValueError("Query is None.  Cannot query nothing")

        return requests.get(dataset_url + "?" + query).json()

    def __get_offset_and_limit(self, query):
        offset = 0
        limit = 0
        params = query.lower().split("&")
        for p in params:
            if "offset" in p:
                offset = p.split("=")[1].strip()
            elif "limit" in p:
                limit = p.split("=")[1].strip()
        return [offset,limit]

    def execute(self, context):
        """
        Execute the operator.
        This will get all the data from the dataset and write it to a file.
        """
        logging.info("Prepping to gather data from SODA")

        paginate = False
        params = self.__get_offset_and_limit(self.soql)
        offset,limit = params[0],params[1]
        if offset != 0 and limit != 0:
            logging.info("Results will be paginated")
            paginate = True
        else:
            logging.info("Results will not be paginated")

        key_id = 1
        while True:

            # Open a name temporary file to store output file until S3 upload
            with NamedTemporaryFile("w") as tmp:

                if self.soql:
                    query_results = self.run_query(self.soql,self.dataset_url)

                if len(query_results) == 0:
                    break

                # output the records from the query to a file
                # the list of records is stored under the "records" key
                logging.info("Writing query results to: {0}".format(tmp.name))

                query_results = [json.dumps(result, ensure_ascii=False) for result in query_results]
                query_results = '\n'.join(query_results)
                tmp.write(query_results)

                # Flush the temp file and upload temp file to S3
                tmp.flush()

                dest_s3 = S3Hook(self.s3_conn_id)

                dest_s3.load_file(
                    filename=tmp.name,
                    key=self.s3_key + "-"+str(key_id),
                    bucket_name=self.s3_bucket,
                    replace=True
                )

                #dest_s3.connection.close()

                tmp.close()

            if not paginate:
                break
            else:
                offset+=limit
            key_id+=1

        logging.info("Query finished!")
