# Trino

## Purpose

Query engine used in front of the Iceberg catalog and LocalStack-backed S3 content.

## Files

- trino-values.yaml: Helm values for Trino deployment

## Deploy

```bash
helm repo add trino https://trinodb.github.io/charts/
helm repo update
helm upgrade --install trino trino/trino \
  -n bigdata \
  -f /home/obase/works/k8s/Trino/trino-values.yaml
```

## Port-forward

```bash
kubectl port-forward --address 0.0.0.0 svc/trino 8090:8090 -n bigdata
```

Open:

- http://localhost:8090

## Cockpit / UI

The UI is available at:

```text
http://localhost:8090/ui/
```

## Important notes

- The Iceberg catalog config targets Polaris REST service
- S3 properties must use Trino-compatible names such as `hive.s3.*`
- If configuration properties are reported as unused, check the correct catalog namespace

## Production hardening

- Configure TLS termination / ingress
- Add resource limits and autoscaling
- Use a managed catalog and object store in production
- Secure credentials in Kubernetes Secrets rather than embedding them in config

# STS olmadan values yaml
"""
image:
  repository: trinodb/trino
  tag: "449"
  pullPolicy: IfNotPresent

server:
  workers: 1
  node:
    environment: production
    dataDir: /data/trino
  config:
    query.max-memory-per-node: "512MB"
    query.max-memory: "1GB"
    http-server.http.port: "8090"
  jvm:
    maxHeapSize: "1536M"

coordinator:
  enabled: true
  resources:
    requests:
      cpu: "500m"
      memory: "1536Mi"
    limits:
      cpu: "1500m"
      memory: "2500Mi"

worker:
  enabled: true
  replicas: 1
  resources:
    requests:
      cpu: "500m"
      memory: "1536Mi"
    limits:
      cpu: "1500m"
      memory: "2500Mi"

service:
  type: ClusterIP
  port: 8090

additionalCatalogs:
  iceberg: |
    connector.name=iceberg
    iceberg.catalog.type=rest
    iceberg.rest-catalog.uri=http://polaris-catalog-service.bigdata.svc.cluster.local:8181/api/catalog
    iceberg.rest-catalog.warehouse=polaris
    iceberg.rest-catalog.security=OAUTH2
    iceberg.rest-catalog.oauth2.token=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJwb2xhcmlzIiwic3ViIjoidHJpbm9fdXNlciIsImlhdCI6MTc4ODI1ODUyNywiZXhwIjoxNzg4MjYyMTI3LCJqdGkiOiJiMDYyMDNhOC03N2I4LTQwNmYtOTM4OC02NzYzMjhiZjg5MmEiLCJhY3RpdmUiOnRydWUsImNsaWVudF9pZCI6ImQwMjA2ZjFhYzYwMjFlZTYiLCJwcmluY2lwYWxJZCI6ODQ2ODM1NjA5ODU3MzcwNTczOCwic2NvcGUiOiJQUklOQ0lQQUxfUk9MRTpBTEwifQ.K2v_evOdXZFVhP7fiv4yzQVppo98bmr3f2GtB-IA7ZAzTLw5h2jYMJv6VvFJKuNf3vhg0NTSvyw0withKpIkOOZ_iJMLOKIbGyTO5f64QnW8AS-vB06z5vQ1osucPILjTSAugbx_g2LlX2eZnJ8c6pDGI0qD8zmzf9t4_DaJ_XHrthBrJy5cKfKGsVFCqYwCJ7dKsDPj6-8Z_I3xkxzybaRCf2PwZiFY9lOrCWIAeaxH2tjQhWQ13sUFIXKn0jxRrDWXw1rmsydTHuO-3ITQMsWqu2QcrF2wIS9Mj0iu8SYW9c5l2-Z7Y_3-LgXM1wc8D9dxSMDLKKr8iERY-n_ZCA
    iceberg.rest-catalog.vended-credentials-enabled=false

    # LocalStack Native S3 Ayarları
    fs.native-s3.enabled=true
    s3.endpoint=http://localstack-service.bigdata.svc.cluster.local:4566
    s3.path-style-access=true
    s3.region=us-east-1
    s3.aws-access-key=test
    s3.aws-secret-key=test

    """