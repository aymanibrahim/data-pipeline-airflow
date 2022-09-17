from datetime import datetime
from airflow import DAG
from airflow.operators import CreateTableOperator


dag = DAG('create_tables_dag',
          description='Create tables in Redshift',
          owner='sparkify',
          start_date=datetime.now(),
          schedule_interval='@once',
          max_active_runs=1
)

create_tables = CreateTableOperator(
    task_id='create_tables',
    dag=dag,
    redshift_conn_id='redshift'    
)
