from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id,
                 table_to,
                 query,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table_to
        self.query = query
        self.autocommit = True

    def execute(self, context):
        self.log.info(f'Initializing INSERT into {self.table} table...')
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        insert_sql = f"""
            INSERT INTO {self.table}
            {self.query};
            COMMIT;
        """
        self.hook.run(insert_sql, self.autocommit)
        self.log.info(f"INSERT into {self.table} command complete!")