# Apache Polaris – Resmi Admin Tool ile Veritabanını Bootstrap Etme

Bu doküman, PostgreSQL üzerinde bulunan eski `polaris_schema` şemasını temizleyerek **Apache Polaris'in resmi Admin Tool** aracını Kubernetes `Job` olarak çalıştırır ve Polaris veritabanı şemasını yeniden oluşturur.

> **Önemli:** Aşağıdaki işlemler `polaris_schema` şemasını `CASCADE` ile tamamen siler. Üretim ortamında uygulamadan önce mevcut verilerin yedeğini aldığınızdan emin olun.

---

## 1. Eski `polaris_schema` Şemasını Sıfırlayın

Öncelikle PostgreSQL'de daha önce oluşturulmuş veya yarım kalmış `polaris_schema` şemasını temizleyin.

### PostgreSQL Pod'unu Bulun

```bash
POSTGRES_POD=$(kubectl get pod -n bigdata \
  -l app.kubernetes.io/name=postgresql \
  -o jsonpath="{.items[0].metadata.name}")
```

### Şemayı Silin

```bash
kubectl exec -i -n bigdata $POSTGRES_POD -- \
  bash -c 'PGPASSWORD=$POSTGRES_PASSWORD psql \
  -U postgres \
  -d polaris_db \
  -c "DROP SCHEMA IF EXISTS polaris_schema CASCADE;"'
```

> `CASCADE` kullanıldığı için `polaris_schema` içerisindeki tablolar, sequence'ler ve bağlı nesneler birlikte silinir.

---

## 2. Resmi Polaris Bootstrap Job'ını Başlatın

Polaris'in resmi şema oluşturucusu olan **Admin Tool**'u Kubernetes `Job` olarak çalıştırın.

Aşağıdaki manifesti doğrudan terminale yapıştırabilirsiniz:

```bash
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
```

### Kullanılan Bağlantı Bilgileri

| Parametre | Değer |
|---|---|
| **Database** | `polaris_db` |
| **Host** | `airflow-postgresql.bigdata.svc.cluster.local` |
| **Port** | `5432` |
| **Username** | `postgres` |
| **Persistence Type** | `relational-jdbc` |
| **Realm** | `POLARIS` |

Bootstrap işlemi sırasında:

- PostgreSQL veritabanına bağlantı kurulur.
- `POLARIS` realm'i oluşturulur.
- Polaris için gerekli tablolar oluşturulur.
- Gerekli veritabanı yapısı hazırlanır.

---

## 3. Bootstrap İşleminin Tamamlandığını Doğrulayın

Job'ın başarıyla tamamlanmasını bekleyin:

```bash
kubectl wait \
  --for=condition=complete \
  job/polaris-bootstrap-job \
  -n bigdata \
  --timeout=60s
```

### Job Loglarını Görüntüleyin

```bash
kubectl logs job/polaris-bootstrap-job -n bigdata
```

Başarılı bir bootstrap işleminde loglarda aşağıdakine benzer bir mesaj görülmesi beklenir:

```text
Realm POLARIS bootstrapped successfully
```

### Job Durumunu Kontrol Edin

```bash
kubectl get job polaris-bootstrap-job -n bigdata
```

Başarılı durumda `COMPLETIONS` alanı:

```text
1/1
```

olmalıdır.

---

## 4. Polaris Sunucusunu Yeniden Başlatın

Bootstrap işlemi tamamlandıktan sonra Polaris Catalog deployment'ını yeniden başlatın:

```bash
kubectl rollout restart deployment/polaris-catalog -n bigdata
```

Deployment'ın tekrar hazır olmasını bekleyin:

```bash
kubectl rollout status deployment/polaris-catalog -n bigdata
```

Başarılı durumda aşağıdakine benzer bir çıktı alınır:

```text
deployment "polaris-catalog" successfully rolled out
```

---

## 5. Polaris Pod'unu Kontrol Edin

Polaris pod'larının çalıştığını doğrulayın:

```bash
kubectl get pods -n bigdata -l app=polaris-catalog
```

Pod'ların `Running` durumunda olması beklenir.

### Polaris Loglarını İnceleyin

Gerekirse son 100 log satırını görüntüleyin:

```bash
kubectl logs -n bigdata \
  deployment/polaris-catalog \
  --tail=100
```

Loglarda özellikle:

- PostgreSQL bağlantı hataları
- Schema initialization hataları
- Database authentication hataları
- Migration/bootstrap hataları

bulunmadığını kontrol edin.

---

## 6. PostgreSQL Şemasını Doğrulayın

Bootstrap sonrasında `polaris_schema` şemasının tekrar oluşturulduğunu kontrol edin.

### Şemaları Listeleyin

```bash
kubectl exec -i -n bigdata $POSTGRES_POD -- \
  bash -c 'PGPASSWORD=$POSTGRES_PASSWORD psql \
  -U postgres \
  -d polaris_db \
  -c "\dn"'
```

Çıktıda `polaris_schema` şemasının bulunması gerekir.

### Polaris Tablolarını Listeleyin

```bash
kubectl exec -i -n bigdata $POSTGRES_POD -- \
  bash -c 'PGPASSWORD=$POSTGRES_PASSWORD psql \
  -U postgres \
  -d polaris_db \
  -c "\dt polaris_schema.*"'
```

`polaris_schema` altında Polaris'e ait tabloların görünmesi, bootstrap işleminin PostgreSQL'e başarıyla uygulandığını gösterir.

---

# 7. İşlem Akışı

Bootstrap sürecinin genel akışı aşağıdaki gibidir:

```text
┌───────────────────────────┐
│        PostgreSQL         │
│                           │
│  Eski polaris_schema      │
│  CASCADE ile silinir      │
└─────────────┬─────────────┘
              │
              ▼
┌───────────────────────────┐
│ Polaris Admin Tool Job    │
│                           │
│  • PostgreSQL'e bağlanır  │
│  • POLARIS realm oluşturur│
│  • Tabloları oluşturur    │
└─────────────┬─────────────┘
              │
              ▼
┌───────────────────────────┐
│    Bootstrap Başarılı     │
└─────────────┬─────────────┘
              │
              ▼
┌───────────────────────────┐
│  polaris-catalog restart  │
└─────────────┬─────────────┘
              │
              ▼
┌───────────────────────────┐
│     Polaris Catalog       │
│                           │
│ PostgreSQL üzerindeki     │
│ kalıcı şemayı kullanarak  │
│ çalışmaya başlar          │
└───────────────────────────┘
```

---

# 8. Hızlı Kontrol

Tüm işlemleri tamamladıktan sonra aşağıdaki kontrolleri sırasıyla çalıştırabilirsiniz.

### 1. Bootstrap Job

```bash
kubectl get job polaris-bootstrap-job -n bigdata
```

Beklenen durum:

```text
COMPLETIONS: 1/1
```

### 2. Polaris Pod

```bash
kubectl get pods -n bigdata -l app=polaris-catalog
```

Beklenen durum:

```text
Running
```

### 3. Polaris Deployment

```bash
kubectl rollout status deployment/polaris-catalog -n bigdata
```

Beklenen çıktı:

```text
deployment "polaris-catalog" successfully rolled out
```

### 4. Polaris Logları

```bash
kubectl logs -n bigdata \
  deployment/polaris-catalog \
  --tail=100
```

### 5. PostgreSQL Schema

```bash
kubectl exec -i -n bigdata $POSTGRES_POD -- \
  bash -c 'PGPASSWORD=$POSTGRES_PASSWORD psql \
  -U postgres -d polaris_db \
  -c "\dn"'
```

### 6. Polaris Tabloları

```bash
kubectl exec -i -n bigdata $POSTGRES_POD -- \
  bash -c 'PGPASSWORD=$POSTGRES_PASSWORD psql \
  -U postgres -d polaris_db \
  -c "\dt polaris_schema.*"'
```

---

# 9. Başarı Kriterleri

İşlemin başarıyla tamamlandığını doğrulamak için aşağıdaki koşulların sağlanması gerekir:

- [x] Eski `polaris_schema` temizlendi.
- [x] `polaris-bootstrap-job` başarıyla tamamlandı.
- [x] Job `COMPLETIONS` değeri `1/1` oldu.
- [x] Polaris realm'i `POLARIS` oluşturuldu.
- [x] `polaris_schema` yeniden oluşturuldu.
- [x] Polaris tabloları PostgreSQL'de görünüyor.
- [x] `polaris-catalog` deployment'ı başarıyla yeniden başlatıldı.
- [x] Polaris pod'u `Running` durumunda.
- [x] Polaris loglarında PostgreSQL/schema initialization hatası yok.

---

## Sonuç

Tüm kontroller başarılıysa **Polaris veritabanı bootstrap işlemi tamamlanmış** ve **Polaris Catalog PostgreSQL üzerindeki kalıcı `polaris_schema` şemasını kullanacak şekilde çalışıyor** demektir.

> **Not:** Bu dokümandaki PostgreSQL kullanıcı adı, parola ve Kubernetes servis adresleri mevcut ortamınızdaki değerlerdir. Farklı bir ortamda çalıştırmadan önce bağlantı bilgilerini kontrol edin.
