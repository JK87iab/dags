import airflow
from airflow import DAG

# default arguments
args = {
    'owner': 'Airflow',
    'email': ['<your-email-address>'],
    'email_on_failure' : True,
    'depends_on_past': False,
    'databricks_conn_id': 'adb_workspace'
}

# DAG with Context Manager
# refer to https://airflow.apache.org/docs/stable/concepts.html?highlight=connection#context-manager
