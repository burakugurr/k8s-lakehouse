from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

default_args = {
    "owner": "data_engineering",
    "start_date": datetime(2026, 9, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="wallet_tracker_statistics",
    default_args=default_args,
    description="KubernetesPodOperator ile Günlük Wallet Tracker ve Analitik Tablo İşlemleri",
    schedule="0 21 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["wallet-tracker", "spark", "iceberg", "kubernetes"],
) as dag:

    # Ortak DAG klasörünü bağlamak için PVC tanımı
    volume = k8s.V1Volume(
        name="dags-volume",
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
            claim_name="airflow-dags-pvc"
        ),
    )
    volume_mount = k8s.V1VolumeMount(
        name="dags-volume", 
        mount_path="/opt/airflow/dags", 
        read_only=True
    )

    # Ivy dizini ve ortam değişkenleri
    env_vars = [
        k8s.V1EnvVar(name="HOME", value="/tmp"),
        k8s.V1EnvVar(name="SPARK_USER", value="spark"),
        k8s.V1EnvVar(name="HADOOP_USER_NAME", value="spark"),
        # LocalStack AWS Kimlikleri ve ZORUNLU Endpoint Yönlendirmeleri
        k8s.V1EnvVar(name="AWS_ACCESS_KEY_ID", value="test"),
        k8s.V1EnvVar(name="AWS_SECRET_ACCESS_KEY", value="test"),
        k8s.V1EnvVar(name="AWS_REGION", value="us-east-1"),
        k8s.V1EnvVar(name="AWS_ENDPOINT_URL", value="http://localstack-service.bigdata.svc.cluster.local:4566"),
        k8s.V1EnvVar(name="AWS_ENDPOINT_URL_S3", value="http://localstack-service.bigdata.svc.cluster.local:4566")
    ]
# Task 1: Genel İstatistikler Job'ı
    spark_wallet_tracker_task = KubernetesPodOperator(
        task_id="wallet_tracker_general_stat_job",
        namespace="bigdata",
        image="apache/spark:3.5.0",
        image_pull_policy="IfNotPresent",
        cmds=["/bin/bash", "-cx"],
        arguments=[
            "/opt/spark/bin/spark-submit "
            "--master local[*] "
            # EKSİK OLAN JDBC, HADOOP VE ICEBERG 1.5.0 PAKETLERİ BURAYA EKLENDİ
            "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.iceberg:iceberg-aws-bundle:1.5.0,org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            "--conf spark.driver.memory=2g "
            "--conf spark.sql.adaptive.enabled=true "
            "--conf spark.driver.extraJavaOptions=-Divy.home=/tmp/.ivy2 "
            "/opt/airflow/dags/scripts/wallet_tracker_general_stat_job.py "
            "--target-date {{ ds }}"
        ],
        env_vars=env_vars,
        volumes=[volume],
        volume_mounts=[volume_mount],
        name="wallet-tracker-general-stat-pod",
        is_delete_operator_pod=False,
        get_logs=True,
        in_cluster=True,
    )
    
    # Task 2: Günlük Analitik Tablo Job'ı
    run_other_tables = KubernetesPodOperator(
        task_id="wallet_tracker_daily_stat",
        namespace="bigdata",
        image="apache/spark:3.5.0",
        image_pull_policy="IfNotPresent",
        cmds=["/bin/bash", "-cx"],
        arguments=[
            "/opt/spark/bin/spark-submit "
            "--master local[*] "
            # EKSİK OLAN JDBC, HADOOP VE ICEBERG 1.5.0 PAKETLERİ BURAYA EKLENDİ
            "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.iceberg:iceberg-aws-bundle:1.5.0,org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            "--conf spark.driver.memory=2g "
            "--conf spark.sql.adaptive.enabled=true "
            "--conf spark.driver.extraJavaOptions=-Divy.home=/tmp/.ivy2 "
            "/opt/airflow/dags/scripts/wallet_tracker_daily_stat.py "
            "--target-date {{ ds }}"
        ],
        env_vars=env_vars,
        volumes=[volume],
        volume_mounts=[volume_mount],
        name="wallet-tracker-daily-stat",
        random_name_suffix=True,  # Çakışmaları önlemek için
        is_delete_operator_pod=False,
        get_logs=True,
        in_cluster=True,
    )
    spark_wallet_tracker_task >> run_other_tables