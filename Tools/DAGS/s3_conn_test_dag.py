from airflow.decorators import dag, task
from datetime import datetime
import time
import logging

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['test', 'logging'],
    dag_id='s3_log_test_dag'
)
def s3_log_test():

    @task
    def print_test_logs():
        logger = logging.getLogger('airflow.task')
        logger.info('==================================================')
        logger.info('🚀 S3 UZAK LOGLAMA TESTI BASLIYOR 🚀')
        logger.info('==================================================')
        
        print('Standart print() fonksiyonu ile yazilan log (Stdout)')
        
        for i in range(1, 6):
            logger.info(f'Test adimi {i} isleniyor...')
            time.sleep(1)
            
        logger.info('==================================================')
        logger.info('✅ TEST TAMAMLANDI! BUNU GORUYORSANIZ S3 LOGLAMA KUSURSUZ CALISIYOR!')
        logger.info('==================================================')

    print_test_logs()

s3_log_test()