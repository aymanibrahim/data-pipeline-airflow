from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """Check data quality for tables"""

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 tables,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.tables = tables

    def execute(self, context):
        """Check data quality for tables"""
        redshift_hook = PostgresHook(self.conn_id)
                
        for table in self.tables:
            self.log.info(f"Checking data quality for {table} table")
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"{table} table has no results")
            if records[0][0] < 1:
                raise ValueError(f"{table} table has no rows")
            self.log.info(f"Data quality check for {table} table passed with {records[0][0]} records")