from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tests=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests or []

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        failing_tests = []
        for test in self.tests:
            sql = test.get("table")
            expected_result = test.get("return")
            
            try:
                records = redshift.get_records(sql)
                if len(records) < 1 or len(records[0]) < 1:
                    failing_tests.append(f"Data quality check failed. {sql} returned no results")
                    continue
                    
                num_records = records[0][0]
                if num_records != expected_result:
                    failing_tests.append(f"Data quality check failed. {sql} returned {num_records}, expected {expected_result}")
                else:
                    self.log.info(f"Data quality check passed. {sql} returned {num_records}")
                    
            except Exception as e:
                failing_tests.append(f"Data quality check failed. {sql} raised error: {e}")

        if failing_tests:
            for failure in failing_tests:
                self.log.error(failure)
            raise ValueError('Data quality checks failed')
            
        self.log.info('All data quality checks passed')