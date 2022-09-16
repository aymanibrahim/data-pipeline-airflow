from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """Load Dimension table operator"""
        
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id="",                 
                 table="",
                 query="",
                 truncate="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.query = query
        self.truncate = truncate 

    def execute(self, context):
        """Load Dimension table"""
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        # target table is emptied before the load
        if self.truncate:
            self.log.info(f"Clearing data from {self.table} table")
            redshift.run(f"TRUNCATE {self.table}")
        
        self.log.info('Loading {self.table} Dimension table')
        redshift.run(f"INSERT INTO {self.table} {self.query}")
        self.log.info(f'{self.table} Dimension table loading is completed')
