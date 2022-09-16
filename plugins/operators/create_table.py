from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTableOperator(BaseOperator):
    """Create tables in Redshift operator"""
    ui_color = '#358140'

    @apply_defaults
    def __init__(self, redshift_conn_id = "", *args, **kwargs):
        
        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        
    def execute(self, context):
        """Create tables in Redshift"""
        self.log.info('Creating Postgres SQL Hook for Redshift')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info('Creating tables in Redshift')
        sql_stmt =  open('/home/workspace/airflow/create_tables.sql', 'r').read()
        redshift.run(sql_stmt)
        
        self.log.info("Tables are created ")