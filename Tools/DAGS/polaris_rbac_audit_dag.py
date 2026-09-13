from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import (
    KubernetesPodOperator,
)
from kubernetes.client import models as k8s

default_args = {
    "owner": "data_engineering",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="polaris_rbac_audit_spark_dag",
    default_args=default_args,
    description="KubernetesPodOperator ile Spark Üzerinde Polaris RBAC Denetimi",
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["polaris", "rbac", "spark", "kubernetes"],
) as dag:

  volume = k8s.V1Volume(
      name="dags-volume",
      persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
          claim_name="airflow-dags-pvc"
      ),
  )
  volume_mount = k8s.V1VolumeMount(
      name="dags-volume", mount_path="/opt/airflow/dags", read_only=True
  )

  # HOME ve Ivy kök dizinini /tmp olarak belirten ortam değişkenleri
  env_vars = [
      k8s.V1EnvVar(name="HOME", value="/tmp"),
      k8s.V1EnvVar(name="SPARK_USER", value="spark"),
  ]

  spark_rbac_task = KubernetesPodOperator(
      task_id="spark_polaris_rbac_audit",
      namespace="bigdata",
      image="apache/spark-py:v3.4.0",
      cmds=["/opt/spark/bin/spark-submit"],
      arguments=[
          "--master",
          "local[*]",
          "--conf",
          "spark.driver.memory=1g",
          "--conf",
          "spark.sql.adaptive.enabled=true",
          "--conf",
          "spark.driver.extraJavaOptions=-Divy.home=/tmp/.ivy2",
          "/opt/airflow/dags/scripts/all_user_list_spark.py",
      ],
      env_vars=env_vars,
      volumes=[volume],
      volume_mounts=[volume_mount],
      name="polaris-rbac-audit-spark",
      is_delete_operator_pod=True,
      get_logs=True,
      in_cluster=True,
  )

  spark_rbac_task