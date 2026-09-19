# Minikube Lakehouse Projesi

Bu proje, Minikube üzerinde çalışan, Apache Iceberg tablo formatını kullanan lokal bir Lakehouse mimarisidir. 

## Kullanılan Araçlar
- **Minikube & Helm:** Kubernetes altyapısı ve paket yönetimi.
- **Localstack:** S3 uyumlu Object Storage (Veri gölü).
- **Apache Spark:** Veri işleme motoru.
- **Apache Iceberg:** Lakehouse (ACID) tablo formatı.
- **Apache Airflow:** Zamanlanmış görevler.
- **Jupyter Notebook:** Analitik görevler.


## Kurulum Adımları

1. **Starter.sh Çalıştırın:**
   ```bash
   bash starter.sh
   ```
   Bu sizin için gerekli tüm hazırlıkları yapacaktır.

```mermaid
flowchart TD

subgraph group_platform["Kubernetes Platform"]
  node_minikube{{"Minikube cluster<br/>Kubernetes runtime"}}
end

subgraph group_data["Lakehouse Data Plane"]
  node_localstack[("LocalStack S3<br/>S3-compatible storage")]
  node_polaris[("Polaris catalog")]
  node_catalog_config["Catalog configuration<br/>catalog bootstrap<br/>[catalog.json]"]
end

subgraph group_compute["Processing &amp; Operations"]
  node_spark{{"Apache Spark<br/>batch compute engine"}}
  node_airflow{{"Apache Airflow<br/>workflow scheduler"}}
  node_wallet_dag["Wallet tracker DAG<br/>scheduled Spark workflow"]
  node_wallet_jobs["Wallet statistic jobs<br/>Spark job scripts"]
  node_maintenance_dags["Audit &amp; cleanup workflows<br/>operational DAGs"]
end

subgraph group_access["Query &amp; User Access"]
  node_trino{{"Trino<br/>SQL query engine<br/>[trino-values.yaml]"}}
  node_dremio{{"Dremio<br/>semantic query layer<br/>[dremio.yaml]"}}
  node_hue["Hue<br/>SQL user interface<br/>[hue.yaml]"]
  node_filestash["Filestash<br/>storage browser UI"]
end

subgraph group_validation["Interactive Validation"]
  node_jupyter["Jupyter Spark workspace<br/>interactive analysis<br/>[jupyter-spark.yaml]"]
  node_spark_iceberg_test["Spark Iceberg test<br/>end-to-end validation"]
  node_polaris_connection_test["Polaris connection test<br/>catalog validation notebook"]
end

node_bootstrap["Bootstrap installer<br/>shell entry point<br/>[starter.sh]"]

node_bootstrap -->|"provisions"| node_minikube
node_minikube -->|"runs"| node_localstack
node_minikube -->|"runs"| node_polaris
node_minikube -->|"runs"| node_spark
node_minikube -->|"runs"| node_airflow
node_polaris -->|"configured by"| node_catalog_config
node_polaris -->|"catalogs data in"| node_localstack
node_airflow -->|"schedules"| node_wallet_dag
node_wallet_dag -->|"invokes"| node_wallet_jobs
node_wallet_jobs -->|"runs on"| node_spark
node_airflow -->|"schedules"| node_maintenance_dags
node_maintenance_dags -->|"audits"| node_polaris
node_spark -->|"uses catalog"| node_polaris
node_trino -->|"uses catalog"| node_polaris
node_dremio -->|"uses catalog"| node_polaris
node_hue -.->|"queries through"| node_trino
node_filestash -->|"browses"| node_localstack
node_jupyter -->|"interactive jobs"| node_spark
node_spark_iceberg_test -->|"validates"| node_spark
node_polaris_connection_test -->|"validates"| node_polaris

click node_bootstrap "https://github.com/burakugurr/k8s-lakehouse/blob/main/starter.sh"
click node_minikube "https://github.com/burakugurr/k8s-lakehouse/blob/main/Tools/minikube-config.txt"
click node_localstack "https://github.com/burakugurr/k8s-lakehouse/blob/main/Tools/LocalStack/localstack-deployment.yaml"
click node_polaris "https://github.com/burakugurr/k8s-lakehouse/blob/main/Tools/Polaris/polaris-deployment.yaml"
click node_catalog_config "https://github.com/burakugurr/k8s-lakehouse/blob/main/Tools/Polaris/catalog.json"
click node_airflow "https://github.com/burakugurr/k8s-lakehouse/blob/main/Tools/Airflow/airflow-values.yaml"
click node_wallet_dag "https://github.com/burakugurr/k8s-lakehouse/blob/main/Tools/DAGS/wallet_tracker_dag.py"
click node_wallet_jobs "https://github.com/burakugurr/k8s-lakehouse/blob/main/Tools/DAGS/scripts/wallet_tracker_daily_stat.py"
click node_maintenance_dags "https://github.com/burakugurr/k8s-lakehouse/blob/main/Tools/DAGS/polaris_rbac_audit_dag.py"
click node_trino "https://github.com/burakugurr/k8s-lakehouse/blob/main/Tools/Trino/trino-values.yaml"
click node_dremio "https://github.com/burakugurr/k8s-lakehouse/blob/main/Tools/Dremio/dremio.yaml"
click node_hue "https://github.com/burakugurr/k8s-lakehouse/blob/main/Tools/Hue/hue.yaml"
click node_jupyter "https://github.com/burakugurr/k8s-lakehouse/blob/main/Tools/Jupyter/jupyter-spark.yaml"
click node_spark_iceberg_test "https://github.com/burakugurr/k8s-lakehouse/blob/main/lakehouse_doc/spark_iceberg_test.py"
click node_polaris_connection_test "https://github.com/burakugurr/k8s-lakehouse/blob/main/lakehouse_doc/polaris_conn_test.ipynb"

classDef toneNeutral fill:#f8fafc,stroke:#334155,stroke-width:1.5px,color:#0f172a
classDef toneBlue fill:#dbeafe,stroke:#2563eb,stroke-width:1.5px,color:#172554
classDef toneAmber fill:#fef3c7,stroke:#d97706,stroke-width:1.5px,color:#78350f
classDef toneMint fill:#dcfce7,stroke:#16a34a,stroke-width:1.5px,color:#14532d
classDef toneRose fill:#ffe4e6,stroke:#e11d48,stroke-width:1.5px,color:#881337
classDef toneIndigo fill:#e0e7ff,stroke:#4f46e5,stroke-width:1.5px,color:#312e81
classDef toneTeal fill:#ccfbf1,stroke:#0f766e,stroke-width:1.5px,color:#134e4a
class node_minikube toneBlue
class node_localstack,node_polaris,node_catalog_config toneAmber
class node_spark,node_airflow,node_wallet_dag,node_wallet_jobs,node_maintenance_dags toneMint
class node_trino,node_dremio,node_hue,node_filestash toneRose
class node_jupyter,node_spark_iceberg_test,node_polaris_connection_test toneIndigo
class node_bootstrap toneNeutral
```
