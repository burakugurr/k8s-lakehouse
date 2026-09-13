# Jupyter

## Purpose

Jupyter Notebook environment for exploratory data work and Spark testing.

## Files

- jupyter-pvc.yaml: persistent volume claim for notebook workspace
- jupyter-spark.yaml: Deployment + Service for Jupyter

## Deploy

```bash
kubectl apply -n bigdata -f /home/obase/works/k8s/Jupyter/jupyter-pvc.yaml
kubectl apply -n bigdata -f /home/obase/works/k8s/Jupyter/jupyter-spark.yaml
```

## Port-forward

```bash
kubectl port-forward --address 0.0.0.0 svc/jupyter-spark-service 8888:8888 -n bigdata
```

Open:

- http://localhost:8888

Token:

- bigdata

## Connectivity checks

From inside the cluster, validate access to Polaris and LocalStack:

```bash
kubectl exec -n bigdata deploy/jupyter-spark -- python - <<'PY'
import requests
for u in [
    'http://polaris-catalog-service.bigdata.svc.cluster.local:8181/q/health',
    'http://localstack-service.bigdata.svc.cluster.local:4566/_localstack/health'
]:
    r = requests.get(u, timeout=10)
    print(u, r.status_code, r.text[:200])
PY
```

## Important notes

- Notebook storage is on a PVC for local persistence
- The environment is intended for development and analysis, not production authoring
- Avoid exposing raw notebook ports publicly in production

## Production hardening

- Add ingress with auth and TLS
- Use a dedicated user and RBAC policy
- Bind to a managed persistent volume
- Add CPU / memory requests and node affinity
- Use secret-backed credentials instead of embedded tokens
