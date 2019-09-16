from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id,
                 table_to,
                 query,
                 truncate_before = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table_to
        self.query = query
        self.truncate_before = truncate_before
        self.autocommit = True

    def execute(self, context):
        self.log.info(f'Loading dimension {self.table} table...')
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_before:
            insert_sql = f"""
                TRUNCATE TABLE {self.table};
                INSERT INTO {self.table}
                {self.query};
                COMMIT;
            """
        else:
            insert_sql = f"""
                INSERT INTO {self.table}
                {self.query};
                COMMIT;
            """
        self.hook.run(insert_sql, self.autocommit)
        self.log.info(f"Dimension {self.table} table loading complete!")
