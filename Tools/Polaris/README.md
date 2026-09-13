# Polaris

## Purpose

Apache Polaris acts as the Iceberg REST catalog for the Lakehouse stack.

## Files

- polaris-deployment.yaml: service and deployment config
- catalog.json: catalog service metadata
- NewDB_create.md: database creation notes

## Deploy

```bash
kubectl apply -n bigdata -f /home/obase/works/k8s/Polaris/polaris-deployment.yaml
```

## Port-forward

```bash
kubectl port-forward --address 0.0.0.0 svc/polaris-catalog-service 8181:8181 -n bigdata
```

Open:

- http://localhost:8181

## Health check

```bash
curl http://localhost:8181/q/health
```

## Database requirement

If startup fails because the database is missing:

```bash
kubectl exec -n bigdata airflow-postgresql-0 -- env PGPASSWORD=postgres /opt/bitnami/postgresql/bin/psql -h localhost -U postgres -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE polaris_db;"
kubectl delete pod -n bigdata -l app=polaris-catalog
```

## Important notes

- Uses PostgreSQL from the Airflow chart as the backing relational store
- Requires `polaris_db` before app boot
- Should be configured with root credentials and secure token setup in production

## Production hardening

- Move to external managed Postgres
- Use secrets for root identity and credentials
- Add TLS and ingress protection
- Restrict catalog networking to internal services only


# Polaris üzerindeki Rol Tabanlı Erişim Denetimi (RBAC) modeli 3 ana katmandan oluşur:

Principal: Kimliği temsil eden kullanıcı veya servis hesabı (Örn: etl_service, bi_analyst).

Principal Role: Principal'a doğrudan atanan genel kimlik rolü (Örn: etl_principal_role, analyst_principal_role).

Catalog Role: Belirli bir katalog içinde kalan ve spesifik kaynaklara (Catalog, Namespace, Table) erişim tanımlayan rol (Örn: raw_data_writer, reporting_reader).