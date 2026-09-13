Resmi Admin Tool ile Veritabanını Bootstrap Etme

Bu adımlar, PostgreSQL üzerinde bulunan eski polaris_schema şemasını temizleyip, Apache Polaris'in resmi Admin Tool aracını Kubernetes Job olarak çalıştırarak veritabanı şemasını yeniden oluşturur.

1. Eski polaris_schema Şemasını Sıfırlayın

Öncelikle PostgreSQL'de daha önce oluşturulmuş veya yarım kalmış polaris_schema şemasını temizleyin.

Terminalde aşağıdaki komutu çalıştırın:

POSTGRES_POD=$(kubectl get pod -n bigdata \
  -l app.kubernetes.io/name=postgresql \
  -o jsonpath="{.items[0].metadata.name}")

kubectl exec -i -n bigdata $POSTGRES_POD -- \
  bash -c 'PGPASSWORD=$POSTGRES_PASSWORD psql \
  -U postgres \
  -d polaris_db \
  -c "DROP SCHEMA IF EXISTS polaris_schema CASCADE;"'


Not: CASCADE kullanıldığı için polaris_schema içerisindeki tüm tablolar, sequence'ler ve bağlı nesneler birlikte silinir.

2. Resmi Polaris Bootstrap Job'ını Başlatın

Polaris'in resmi şema oluşturucusu olan Admin Tool'u Kubernetes Job olarak çalıştırın.

Aşağıdaki manifesti doğrudan terminale yapıştırabilirsiniz:

cat << 'EOF' | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: polaris-bootstrap-job
  namespace: bigdata
spec:
  ttlSecondsAfterFinished: 60
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: polaris-admin
          image: apache/polaris-admin-tool:latest
          imagePullPolicy: IfNotPresent
          args:
            - "bootstrap"
            - "-r"
            - "POLARIS"
            - "-c"
            - "POLARIS,polaris_root,polaris_secret123"
          env:
            - name: POLARIS_PERSISTENCE_TYPE
              value: "relational-jdbc"

            - name: QUARKUS_DATASOURCE_DB_KIND
              value: "postgresql"

            - name: QUARKUS_DATASOURCE_JDBC_URL
              value: "jdbc:postgresql://airflow-postgresql.bigdata.svc.cluster.local:5432/polaris_db"

            - name: QUARKUS_DATASOURCE_USERNAME
              value: "postgres"

            - name: QUARKUS_DATASOURCE_PASSWORD
              value: "postgres"
EOF


Bu Job aşağıdaki bilgileri kullanarak PostgreSQL'e bağlanır:

Database: polaris_db
Host: airflow-postgresql.bigdata.svc.cluster.local
Port: 5432
Username: postgres
Persistence: relational-jdbc
Realm: POLARIS

Bootstrap sırasında POLARIS realm'i ve gerekli Polaris tabloları oluşturulur.

3. Bootstrap İşleminin Tamamlandığını Doğrulayın

Job'ın başarıyla tamamlanmasını bekleyin:

kubectl wait \
  --for=condition=complete \
  job/polaris-bootstrap-job \
  -n bigdata \
  --timeout=60s


Ardından Job loglarını görüntüleyin:

kubectl logs job/polaris-bootstrap-job -n bigdata


Başarılı bir bootstrap işleminde loglarda aşağıdakine benzer bir mesaj görmelisiniz:

Realm POLARIS bootstrapped successfully


Job'ın durumunu ayrıca kontrol etmek için:

kubectl get job polaris-bootstrap-job -n bigdata


Başarılı durumda COMPLETIONS alanı:

1/1


olmalıdır.

4. Polaris Sunucusunu Yeniden Başlatın

Bootstrap işlemi tamamlandıktan sonra Polaris sunucusunu yeniden başlatın.

kubectl rollout restart deployment/polaris-catalog -n bigdata


Deployment'ın tekrar hazır olmasını bekleyin:

kubectl rollout status deployment/polaris-catalog -n bigdata


Başarılı durumda aşağıdakine benzer bir çıktı alınır:

deployment "polaris-catalog" successfully rolled out

5. Polaris Pod'unu Kontrol Edin

Polaris pod'larının çalıştığını doğrulayın:

kubectl get pods -n bigdata -l app=polaris-catalog


Gerekirse Polaris loglarını inceleyin:

kubectl logs -n bigdata \
  deployment/polaris-catalog \
  --tail=100


Pod Running durumunda olmalı ve loglarda PostgreSQL bağlantısı veya schema initialization ile ilgili hata bulunmamalıdır.

6. PostgreSQL Şemasını Doğrulayın

Bootstrap sonrasında polaris_schema şemasının tekrar oluşturulduğunu kontrol edebilirsiniz:

kubectl exec -i -n bigdata $POSTGRES_POD -- \
  bash -c 'PGPASSWORD=$POSTGRES_PASSWORD psql \
  -U postgres \
  -d polaris_db \
  -c "\dn"'


Ardından Polaris tablolarını listeleyin:

kubectl exec -i -n bigdata $POSTGRES_POD -- \
  bash -c 'PGPASSWORD=$POSTGRES_PASSWORD psql \
  -U postgres \
  -d polaris_db \
  -c "\dt polaris_schema.*"'


polaris_schema altında Polaris'e ait tabloların görünmesi bootstrap işleminin veritabanına uygulandığını gösterir.

7. İşlem Akışı

Özet olarak işlem sırası şöyledir:

PostgreSQL
    │
    ├── Eski polaris_schema silinir
    │
    ▼
Polaris Admin Tool Job
    │
    ├── PostgreSQL'e bağlanır
    ├── POLARIS realm'ini oluşturur
    └── Polaris tablolarını oluşturur
    │
    ▼
Bootstrap başarılı
    │
    ▼
polaris-catalog restart
    │
    ▼
Polaris PostgreSQL üzerindeki
kalıcı şemayı kullanarak başlar

8. Hızlı Kontrol

Tüm işlemleri tamamladıktan sonra aşağıdaki kontrolleri sırasıyla çalıştırabilirsiniz:

# Bootstrap Job
kubectl get job polaris-bootstrap-job -n bigdata

# Polaris Pod
kubectl get pods -n bigdata -l app=polaris-catalog

# Polaris Deployment
kubectl rollout status deployment/polaris-catalog -n bigdata

# Polaris logları
kubectl logs -n bigdata deployment/polaris-catalog --tail=100

# PostgreSQL schema
kubectl exec -i -n bigdata $POSTGRES_POD -- \
  bash -c 'PGPASSWORD=$POSTGRES_PASSWORD psql \
  -U postgres -d polaris_db \
  -c "\dn"'


Tüm kontroller başarılıysa Polaris veritabanı bootstrap işlemi tamamlanmış ve Polaris Catalog PostgreSQL üzerindeki kalıcı şemayı kullanacak şekilde çalışıyor demektir.