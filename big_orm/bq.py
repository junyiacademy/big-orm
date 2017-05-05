from retrying import retry
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from httplib2 import Http
from apiclient.discovery import build


class BQHandler:

    def __init__(self, project_id, credential_path):
        self.credential_path = credential_path
        self.project_id = project_id
        scopes = ['https://www.googleapis.com/auth/bigquery']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            credential_path, scopes)
        http_auth = credentials.authorize(Http())
        bigquery = build('bigquery', 'v2', http=http_auth)
        self.bigquery = bigquery

    def retry_if_job_running(result):
        """Return True if we should retry (in this case when result is 'RUNNING'), False otherwise"""
        return result == 'RUNNING'

    @retry(retry_on_result=retry_if_job_running, wait_fixed=1000, stop_max_attempt_number=250)
    def check_bq_job_status(self, jobId):
        job = self.bigquery.jobs().get(projectId=self.project_id, jobId=jobId).execute()
        # print "job progress: %s" % job['status']['state']
        return job['status']['state']

    def query_to_table(self, query, table):
        dataset = table.split('.')[0]
        table = table.split('.')[1]
        job = self.bigquery.jobs().insert(projectId=self.project_id,
                    body={'projectId': self.project_id,
                           'configuration':{
                             "query": {
                                  "query": query,
                                  "destinationTable": {
                                    "projectId": self.project_id,
                                    "datasetId": dataset,
                                    "tableId": table
                                  },
                             "writeDisposition":"WRITE_TRUNCATE",
                             "createDisposition":"CREATE_IF_NEEDED",
                             "allowLargeResults":"True"
                          }}}).execute()
        job_id_start_with = "%s:" % self.project_id
        self.check_bq_job_status(job['id'].replace(job_id_start_with, ""))
        return job['id']

    def get_df(self, query, legacy=True):
        dialect = 'legacy' if legacy else 'standard'
        return pd.read_gbq(query=query, project_id=self.project_id, private_key=self.credential_path, verbose=False, dialect=dialect)

    def df_to_table(self, df, table):
        return df.to_gbq(destination_table=table, project_id=self.project_id, private_key=self.credential_path)

    def get_tb_resource(self, table):
        datasetId = table.split('.')[0]
        tableId = table.split('.')[1]
        return self.bigquery.tables().get(projectId=self.project_id, datasetId=datasetId, tableId=tableId).execute()

    def get_tb_len(self, table):
        response = self.get_tb_resource(table)
        return response["numRows"]

    def get_tb_columns(self, table):
        response = self.get_tb_resource(table)
        return [f['name'] for f in response['schema']['fields']]
