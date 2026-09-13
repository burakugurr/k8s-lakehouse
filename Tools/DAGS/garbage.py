from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    "owner": "data_engineering",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="iceberg_table_maintenance_trino",
    default_args=default_args,
    description="Iceberg GC & Compaction via Trino",
    schedule="0 2 * * *",  # schedule_interval yerine schedule kullanıldı
    catchup=False,
    max_active_runs=1,
) as dag:

  # 1. Compaction: Küçük dosyaları birleştirir
  task_compaction = SQLExecuteQueryOperator(
      task_id="trino_compaction",
      conn_id="trino_default",
      sql="""
            ALTER TABLE iceberg.sts_db.warehouse_test 
            EXECUTE optimize(file_size_threshold => '128MB');
        """,
  )

  # 2. Expire Snapshots: Eski snapshot'ları düşürür (3 günden eski olanlar)
  task_expire_snapshots = SQLExecuteQueryOperator(
      task_id="trino_expire_snapshots",
      conn_id="trino_default",
      sql="""
            ALTER TABLE iceberg.sts_db.warehouse_test 
            EXECUTE expire_snapshots(retention_threshold => '3d');
        """,
  )

  # 3. Remove Orphan Files: Yetim dosyaları siler (S3 GC)
  task_remove_orphans = SQLExecuteQueryOperator(
      task_id="trino_remove_orphans",
      conn_id="trino_default",
      sql="""
            ALTER TABLE iceberg.sts_db.warehouse_test 
            EXECUTE remove_orphan_files(retention_threshold => '1d');
        """,
  )

  task_compaction >> task_expire_snapshots >> task_remove_orphans