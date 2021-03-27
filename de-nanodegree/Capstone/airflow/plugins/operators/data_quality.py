from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id 
        self.dq_checks = dq_checks
        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        error_count = 0
        for check in self.dq_checks:
            sql_statment = check.get('check_sql')
            exp_result   = check.get('expected_result')
            
            records = redshift.get_records(sql_statment)            
            num_records = records[0][0]
            
            if exp_result != num_records:
                error_count += 1
                failing_tests.append(sql)                
             
        if error_count > 0:
            self.log.info('Tests failed')
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed')     
        else:
            self.log.info('Data quality check success')
