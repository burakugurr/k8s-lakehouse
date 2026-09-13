# Airflow

## Purpose

Runs the orchestration layer for the Lakehouse stack.

## Files

- airflow-values.yaml: Helm values for this deployment
- dags-pvc.yaml: persistent volume + claim for DAGs
- airflow-values-test.yaml: alternate config / test overrides

## Deploy

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
helm upgrade --install airflow apache-airflow/airflow \
  -n bigdata \
  -f /home/obase/works/k8s/Airflow/airflow-values.yaml
```

## Port-forward

```bash
kubectl port-forward --address 0.0.0.0 svc/airflow-api-server 8080:8080 -n bigdata
```

Open:

- http://localhost:8080

Credentials:

- username: admin
- password: admin

## DAG directory

The DAG source is located in:

```bash
/home/obase/works/k8s/DAGS
```

Important:

- DAG files must be mounted to the Airflow scheduler pod
- If the scheduler is not refreshing, delete the scheduler pod and wait for restart
- Ensure the DAG PV/PVC is bound before scheduling triggerer/dag processor pods

## Common issues

### Pending triggerer / dag processor

Check:

```bash
kubectl get pvc -n bigdata
kubectl describe node minikube
kubectl get pods -n bigdata | grep airflow
```

### DAGs not showing

Check:

```bash
kubectl exec -n bigdata deploy/airflow-scheduler -- sh -lc "ls -la /opt/airflow/dags"
kubectl logs -n bigdata -l component=scheduler --tail=200
```

If the directory is empty:

```bash
kubectl cp /home/obase/works/k8s/DAGS/. bigdata/$(kubectl get pod -n bigdata -l component=scheduler -o jsonpath='{.items[0].metadata.name}'):/opt/airflow/dags/ -c scheduler
kubectl delete pod -n bigdata -l component=scheduler --grace-period=0 --force
```

## Production hardening

- Set replica count for API server and scheduler in a real cluster
- Use a managed database and not bundled PostgreSQL
- Add TLS termination and secure ingress
- Move the DAG volume to CSI-backed storage
- Add proper RBAC and secret management
- Use external object storage instead of LocalStack in production
