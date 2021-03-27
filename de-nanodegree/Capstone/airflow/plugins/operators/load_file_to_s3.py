import os
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook

class LoadFileIntoS3Operator(BaseOperator):
    
    @apply_defaults
    def __init__(self,
                 filename="",
                 s3_key="",
                 bucket_name="",
                 aws_credentials_id="",
                 airflow_folder="",
                 *args, **kwargs):

        super(LoadFileIntoS3Operator, self).__init__(*args, **kwargs)
        self.filename = filename
        self.s3_key = s3_key
        self.bucket_name = bucket_name
        self.aws_credentials_id = aws_credentials_id
        self.airflow_folder = airflow_folder
        
    def execute(self, context):
        s3 = S3Hook( aws_conn_id = self.aws_credentials_id)
        os.chdir(self.airflow_folder)
        self.log.info("Environment : {}".format(os.getcwd()))
        s3.load_file(filename=self.filename, bucket_name=self.bucket_name, replace=True, key=self.s3_key)

