from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):    
    """Stage data from S3 to Redshift table operator"""

    ui_color = '#358140'
    
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        {}        
    """

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 append_data="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 extra_params="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)        
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.append_data = append_data
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key        
        self.region = region
        self.extra_params = extra_params

    def execute(self, context):
        """Copy data from S3 to Redshift tables"""
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        self.log.info('Creating Postgres SQL Hook for Redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Clearing data from {self.table} Redshift table")
        redshift.run(f"DELETE FROM {self.table}")      
        
        self.log.info(f"Copying data from S3 to {self.table} Redshift table")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
                        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.extra_params
        )
        
        # allow to switch between append-only and delete-load functionality
        if self.append_data == True:
            redshift.run(formatted_sql)
        else:
            sql_stmt = f'DELETE FROM {self.table}' 
            redshift.run(sql_stmt)
            redshift.run(formatted_sql)
                    
        self.log.info(f"Copying {self.table} data from S3 to Redshift is completed")
