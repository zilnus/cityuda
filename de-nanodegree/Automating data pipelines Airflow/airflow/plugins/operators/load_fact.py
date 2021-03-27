from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    insert_sql = """
        INSERT INTO public.{} {}
        {}
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 table_columns="",
                 sql_select="",
                 append_mode=True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.table_columns = table_columns
        self.sql_select = sql_select
        self.append_mode = append_mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.table_columns,
            self.sql_select
        )
        if (self.append_mode):
            self.log.info("Start copying data from staging tables to fact table {}".format(self.table))
            redshift.run(formatted_sql)
        else:
            self.log.info("Clearing data from destination fact table {}".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table)) 
            
            self.log.info("Start copying data from staging tables to fact table {}".format(self.table))
            redshift.run(formatted_sql)            
