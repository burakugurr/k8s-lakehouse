#!/usr/bin/env bash
set -euo pipefail

# =========================================================================
# YAPILANDIRMA
# =========================================================================
CPUS=4
MEMORY=8192mb
DISK_SIZE=20g
K8S_VERSION="${K8S_VERSION:-}"

NAMESPACE="bigdata"
LOGDIR="${LOGDIR:-/tmp/bigdata-ui}"

mkdir -p "$LOGDIR"

# =========================================================================
# MINIKUBE DURUM KONTROLÜ
# =========================================================================
echo "==> Minikube durum kontrolü yapılıyor..."

STATUS=$(minikube status -f '{{.Host}}' 2>/dev/null || echo "Stopped")

if [[ "$STATUS" == *"Running"* ]]; then
    echo "✔ Minikube zaten çalışıyor."
else
    echo "➜ Minikube başlatılıyor..."
    echo "  - CPU: $CPUS | RAM: $MEMORY | Disk: $DISK_SIZE"

    START_ARGS=(
        --cpus="$CPUS"
        --memory="$MEMORY"
        --disk-size="$DISK_SIZE"
    )

    if [ -n "$K8S_VERSION" ]; then
        START_ARGS+=(--kubernetes-version="$K8S_VERSION")
    fi

    minikube start "${START_ARGS[@]}"
fi


# =========================================================================
# YARDIMCI FONKSİYONLAR
# =========================================================================

wait_for_service() {
    local service="$1"
    local timeout="${2:-120s}"

    echo "⏳ ${service} bekleniyor..."

    local selector

    selector=$(
        kubectl get svc "$service" \
            -n "$NAMESPACE" \
            -o jsonpath='{.spec.selector}' 2>/dev/null |
        jq -r 'to_entries | map("\(.key)=\(.value)") | join(",")'
    ) || selector=""

    if [ -z "$selector" ]; then
        echo "⚠️  ${service} için selector bulunamadı."
        return 0
    fi

    if ! kubectl wait \
        --namespace="$NAMESPACE" \
        --for=condition=ready pod \
        --selector="$selector" \
        --timeout="$timeout" >/dev/null 2>&1
    then
        echo "⚠️  Uyarı: ${service} hazır hale gelmedi."
    else
        echo "✔ ${service} hazır."
    fi
}


start_port_forward() {
    local name="$1"
    local service="$2"
    local local_port="$3"
    local target_port="$4"
    local log_file="$LOGDIR/${name}.log"

    if ss -lnt 2>/dev/null |
        awk '{print $4}' |
        grep -q ":${local_port}$"
    then
        echo "✔ ${name}: :${local_port} zaten dinleniyor."
        return 0
    fi

    echo "  ➜ ${name} port forwarding başlatılıyor..."

    nohup kubectl port-forward \
        --address=0.0.0.0 \
        "svc/${service}" \
        "${local_port}:${target_port}" \
        -n "$NAMESPACE" \
        >"$log_file" 2>&1 &

    sleep 1

    if ss -lnt 2>/dev/null |
        awk '{print $4}' |
        grep -q ":${local_port}$"
    then
        echo "✔ ${name}: 0.0.0.0:${local_port} -> ${service}:${target_port}"
    else
        echo "⚠️  ${name} port forwarding başlatılamadı."
        echo "   Log: $log_file"
    fi
}


# =========================================================================
# STAGE 1: S3 BUCKET KONTROLÜ VE OLUŞTURULMASI
# =========================================================================
echo ""
echo "========================================================================="
echo "STAGE 1: S3 BUCKET KONTROLÜ"
echo "========================================================================="

echo "==> S3 Bucket'ları kontrol ediliyor..."

REQUIRED_BUCKETS=(
    "airflow-logs"
    "datalake"
    "warehouse"
)

LOCALSTACK_SELECTOR=$(
    kubectl get svc localstack-service \
        -n "$NAMESPACE" \
        -o jsonpath='{.spec.selector}' 2>/dev/null |
    jq -r 'to_entries | map("\(.key)=\(.value)") | join(",")'
) || LOCALSTACK_SELECTOR=""

if [ -n "$LOCALSTACK_SELECTOR" ]; then

    LOCALSTACK_POD=$(
        kubectl get pod \
            -n "$NAMESPACE" \
            --selector="$LOCALSTACK_SELECTOR" \
            -o jsonpath='{.items[0].metadata.name}' 2>/dev/null
    ) || LOCALSTACK_POD=""

    if [ -n "$LOCALSTACK_POD" ]; then

        echo "  ✔ LocalStack pod: $LOCALSTACK_POD"

        for BUCKET in "${REQUIRED_BUCKETS[@]}"; do

            if ! kubectl exec \
                -n "$NAMESPACE" \
                "$LOCALSTACK_POD" \
                -- awslocal s3api head-bucket \
                --bucket "$BUCKET" >/dev/null 2>&1
            then

                echo "  ➜ '$BUCKET' bucket'ı bulunamadı, oluşturuluyor..."

                kubectl exec \
                    -n "$NAMESPACE" \
                    "$LOCALSTACK_POD" \
                    -- awslocal s3 mb "s3://$BUCKET" >/dev/null

                echo "  ✔ '$BUCKET' başarıyla oluşturuldu."

            else
                echo "  ✔ '$BUCKET' zaten mevcut."
            fi

        done

    else
        echo "⚠️  LocalStack pod'u bulunamadı."
        echo "    S3 bucket kontrolü atlandı."
    fi

else
    echo "⚠️  LocalStack servisi bulunamadı."
    echo "    S3 bucket kontrolü atlandı."
fi


# =========================================================================
# STAGE 2: S3 ARAYÜZÜ - FILESTASH & HUE KONTROLÜ
# =========================================================================
echo ""
echo "========================================================================="
echo "STAGE 2: ARAYÜZ SERVİSLERİ KONTROLÜ (FILESTASH & HUE)"
echo "========================================================================="

echo "==> Filestash (S3 Arayüzü) kontrol ediliyor..."

if ! kubectl get svc s3-ui-service \
    -n "$NAMESPACE" >/dev/null 2>&1
then
    echo "  ➜ Filestash bulunamadı, Kubernetes üzerine kuruluyor..."

    cat <<EOF | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: s3-ui-filestash
  namespace: ${NAMESPACE}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: s3-ui
  template:
    metadata:
      labels:
        app: s3-ui
    spec:
      containers:
      - name: filestash
        image: machines/filestash:latest
        ports:
        - containerPort: 8334
---
apiVersion: v1
kind: Service
metadata:
  name: s3-ui-service
  namespace: ${NAMESPACE}
spec:
  selector:
    app: s3-ui
  ports:
    - protocol: TCP
      port: 8334
      targetPort: 8334
EOF

    wait_for_service "s3-ui-service" "120s"
else
    echo "  ✔ Filestash (S3 Arayüzü) zaten kurulu."
fi

# Hue Kontrolü
echo "==> Apache Hue kontrol ediliyor..."
if kubectl get svc hue-service -n "$NAMESPACE" >/dev/null 2>&1; then
    echo "  ✔ Hue servisi mevcut."
else
    echo "  ⚠️ Hue servisi (hue-service) bulunamadı. Lütfen hue.yaml dosyasını uygulayın."
fi


# =========================================================================
# STAGE 3: PORT FORWARDING
# =========================================================================
echo ""
echo "========================================================================="
echo "STAGE 3: PORT FORWARDING"
echo "========================================================================="

echo "==> Port forwarding başlatılıyor..."

start_port_forward airflow    airflow-api-server   8080 8080
start_port_forward jupyter    jupyter-spark-service 8888 8888
# Not: Jupyter 8888 kullandığı için Hue'yu dışarıya 8889 portu üzerinden veriyoruz
start_port_forward hue        hue-service          8889 8888
start_port_forward trino      trino                8090 8090
start_port_forward polaris    polaris-catalog-service 8181 8181
start_port_forward localstack localstack-service   4566 4566
start_port_forward filestash  s3-ui-service        8334 8334

sleep 3


# =========================================================================
# STAGE 4: POLARIS CATALOG OTOMASYONU
# =========================================================================
echo ""
echo "========================================================================="
echo "STAGE 4: POLARIS CATALOG OTOMASYONU"
echo "========================================================================="

echo "==> Polaris Catalog (Iceberg) ayarları yapılandırılıyor..."

POLARIS_TOKEN=$(
    curl -s \
        -X POST \
        http://localhost:8181/api/catalog/v1/oauth/tokens \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -d "grant_type=client_credentials&client_id=polaris_root&client_secret=polaris_secret123&scope=PRINCIPAL_ROLE:ALL" |
    grep -o '"access_token":"[^"]*' | cut -d'"' -f4 || true
)

if [ -n "$POLARIS_TOKEN" ]; then

    echo "  ✔ Polaris token başarıyla alındı."

    # Katalog mevcut mu kontrol et
    CATALOG_EXISTS=$(curl -s -o /dev/null -w "%{http_code}" -X GET http://localhost:8181/api/management/v1/catalogs/polaris -H "Authorization: Bearer $POLARIS_TOKEN")

    if [ "$CATALOG_EXISTS" -eq 404 ]; then
        echo "  ➜ 'polaris' kataloğu bulunamadı, sıfırdan oluşturuluyor..."
        CREATE_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:8181/api/management/v1/catalogs \
            -H "Authorization: Bearer $POLARIS_TOKEN" \
            -H "Accept: application/json" \
            -H "Content-Type: application/json" \
            -d '{
                "catalog": {
                    "name": "polaris",
                    "type": "INTERNAL",
                    "readOnly": false,
                    "properties": {
                        "default-base-location": "s3://warehouse/",
                        "file-io.impl": "org.apache.iceberg.aws.s3.S3FileIO",
                        "s3.endpoint": "http://localstack-service.bigdata.svc.cluster.local:4566",
                        "s3.path-style-access": "true",
                        "s3.access-key-id": "test",
                        "s3.secret-access-key": "test",
                        "client.region": "us-east-1"
                    },
                    "storageConfigInfo": {
                        "storageType": "S3",
                        "allowedLocations": ["s3://warehouse/", "s3://warehouse/*"]
                    }
                }
            }')

        if [ "$CREATE_STATUS" -eq 200 ] || [ "$CREATE_STATUS" -eq 201 ]; then
            echo "  ✔ 'polaris' kataloğu başarıyla oluşturuldu."
        else
            echo "  ⚠️ Katalog oluşturulurken hata (HTTP $CREATE_STATUS)."
        fi
    else
        echo "  ➜ 'polaris' kataloğu mevcut, ayarları güncelleniyor..."
        CURRENT_VERSION=$(curl -s -X GET http://localhost:8181/api/management/v1/catalogs/polaris -H "Authorization: Bearer $POLARIS_TOKEN" | grep -o '"entityVersion":[0-9]*' | head -1 | cut -d':' -f2 || echo "1")

        UPDATE_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X PUT http://localhost:8181/api/management/v1/catalogs/polaris \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $POLARIS_TOKEN" \
            -d '{
                "currentEntityVersion": '"$CURRENT_VERSION"',
                "catalog": {
                    "name": "polaris",
                    "type": "INTERNAL",
                    "properties": {
                        "default-base-location": "s3://warehouse/",
                        "file-io.impl": "org.apache.iceberg.aws.s3.S3FileIO",
                        "s3.endpoint": "http://localstack-service.bigdata.svc.cluster.local:4566",
                        "s3.path-style-access": "true",
                        "s3.access-key-id": "test",
                        "s3.secret-access-key": "test",
                        "client.region": "us-east-1"
                    },
                    "storageConfigInfo": {
                        "storageType": "S3",
                        "allowedLocations": ["s3://warehouse/", "s3://warehouse/*"]
                    }
                }
            }')

        if [ "$UPDATE_STATUS" -eq 200 ] || [ "$UPDATE_STATUS" -eq 204 ]; then
            echo "  ✔ 'polaris' kataloğu başarıyla güncellendi."
        else
            echo "  ⚠️ Katalog güncellenirken bir sorun oluştu (HTTP $UPDATE_STATUS)."
        fi
    fi

    # Namespace kontrolü/oluşturma
    echo "  ➜ 'wallettracker' namespace kontrolü yapılıyor..."
    NAMESPACE_STATUS=$(
        curl -s \
            -o /dev/null \
            -w "%{http_code}" \
            -X POST \
            http://localhost:8181/api/catalog/v1/polaris/namespaces \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $POLARIS_TOKEN" \
            -d '{ "namespace": ["wallettracker"] }'
    )

    if [ "$NAMESPACE_STATUS" -eq 200 ] ||
       [ "$NAMESPACE_STATUS" -eq 201 ] ||
       [ "$NAMESPACE_STATUS" -eq 204 ] ||
       [ "$NAMESPACE_STATUS" -eq 409 ]
    then
        echo "  ✔ 'wallettracker' namespace hazır."
    else
        echo "  ⚠️ 'wallettracker' namespace kontrolünde hata (HTTP $NAMESPACE_STATUS)."
    fi

else
    echo "⚠️ Polaris token alınamadı."
    echo "   Catalog otomasyonu atlandı."
    echo "   Polaris loglarını kontrol etmeniz önerilir."
fi

# =========================================================================
# STAGE 5: MINIKUBE DASHBOARD
# =========================================================================
echo ""
echo "========================================================================="
echo "STAGE 5: MINIKUBE DASHBOARD"
echo "========================================================================="

echo "==> Minikube Dashboard kontrol ediliyor..."

DASHBOARD_PORT=8001
DASHBOARD_LOG="$LOGDIR/minikube-dashboard.log"

DASHBOARD_STATUS=$(minikube addons list 2>/dev/null |
    awk '$1 == "dashboard" {print $2}' || true)

if [[ "$DASHBOARD_STATUS" != "enabled" ]]; then
    echo "  ➜ Minikube Dashboard addon'u etkin değil."
    echo "  ➜ Dashboard addon'u etkinleştiriliyor..."

    minikube addons enable dashboard

    echo "  ✔ Dashboard addon'u etkinleştirildi."
else
    echo "  ✔ Minikube Dashboard addon'u zaten etkin."
fi

if ss -lnt 2>/dev/null |
    awk '{print $4}' |
    grep -q ":${DASHBOARD_PORT}$"
then
    echo "  ✔ Kubectl proxy zaten çalışıyor."
    echo "    Port: ${DASHBOARD_PORT}"
else
    echo "  ➜ Kubectl proxy başlatılıyor..."
    echo "    Address: 0.0.0.0"
    echo "    Port:    ${DASHBOARD_PORT}"

    nohup kubectl proxy \
        --address=0.0.0.0 \
        --accept-hosts='^.*$' \
        --port="$DASHBOARD_PORT" \
        >"$DASHBOARD_LOG" 2>&1 &

    PROXY_PID=$!
    sleep 3

    if ss -lnt 2>/dev/null |
        awk '{print $4}' |
        grep -q ":${DASHBOARD_PORT}$"
    then
        echo "  ✔ Kubectl proxy başarıyla başlatıldı."
        echo "    PID: $PROXY_PID"
    else
        echo "  ❌ Kubectl proxy başlatılamadı."
        echo "     Log: $DASHBOARD_LOG"
    fi
fi

# =========================================================================
# SUNUCU IP ADRESİ & BİLGİLENDİRME
# =========================================================================

SERVER_IP=$(hostname -I | awk '{print $1}')

echo ""
echo "========================================================================="
echo "                    TÜM SERVİSLER HAZIR"
echo "========================================================================="
echo ""
echo "Sunucu IP: $SERVER_IP"
echo ""
echo "Airflow:"
echo "  http://${SERVER_IP}:8080"
echo ""
echo "Jupyter:"
echo "  http://${SERVER_IP}:8888"
echo "  Token: bigdata"
echo ""
echo "Hue (SQL Editor - Trino):"
echo "  http://${SERVER_IP}:8889"
echo ""
echo "Trino:"
echo "  http://${SERVER_IP}:8090"
echo ""
echo "Polaris:"
echo "  http://${SERVER_IP}:8181"
echo ""
echo "LocalStack:"
echo "  http://${SERVER_IP}:4566"
echo ""
echo "Filestash:"
echo "  http://${SERVER_IP}:8334"
echo ""
echo "Minikube Dashboard:"
echo "  http://${SERVER_IP}:8001/api/v1/namespaces/kubernetes-dashboard/services/http:kubernetes-dashboard:/proxy/"
echo ""
echo "Dashboard Log:"
echo "  $DASHBOARD_LOG"
echo ""
echo "========================================================================="