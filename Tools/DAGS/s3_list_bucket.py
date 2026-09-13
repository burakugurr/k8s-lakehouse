from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import logging

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['s3', 'localstack', 'test'],
    dag_id='s3_directory_lister_dag'
)
def s3_directory_lister():

    @task
    def scan_s3_contents():
        logger = logging.getLogger('airflow.task')
        logger.info('==================================================')
        logger.info('🔍 S3 (LOCALSTACK) İÇERİK TARAMASI BAŞLIYOR 🔍')
        logger.info('==================================================')
        
        try:
            # Daha önce çevre değişkenine (JSON) gömdüğümüz bağlantıyı kullanıyoruz
            hook = S3Hook(aws_conn_id='my_s3_conn')
            boto_client = hook.get_conn()
            
            # S3'teki tüm Bucket'ları listele
            buckets_response = boto_client.list_buckets()
            buckets = [b['Name'] for b in buckets_response.get('Buckets', [])]
            
            if not buckets:
                logger.info("S3'te hiçbir bucket bulunamadı.")
                return
            
            logger.info(f"Sistemde toplam {len(buckets)} bucket bulundu: {buckets}")
            
            # Her bir bucket'ın içine girip dosyaları listele
            for bucket in buckets:
                logger.info('--------------------------------------------------')
                logger.info(f'📁 Bucket: {bucket}')
                
                # Bucket içindeki objeleri (1000 taneye kadar) çekiyoruz
                objects_response = boto_client.list_objects_v2(Bucket=bucket)
                
                if 'Contents' in objects_response:
                    for obj in objects_response['Contents']:
                        size_kb = obj['Size'] / 1024
                        logger.info(f"  └─ 📄 {obj['Key']} ({size_kb:.2f} KB)")
                else:
                    logger.info("  └─ (Bu bucket tamamen boş)")
                    
        except Exception as e:
            logger.error(f'S3 taraması sırasında hata oluştu: {str(e)}')
            raise e
            
        logger.info('==================================================')
        logger.info('✅ S3 TARAMASI TAMAMLANDI!')
        logger.info('==================================================')

    scan_s3_contents()

s3_directory_lister_dag = s3_directory_lister()