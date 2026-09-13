helm install airflow apache-airflow/airflow \
  -n bigdata \
  -f airflow-values.yaml

kubectl port-forward --address 0.0.0.0 svc/airflow-api-server 8080:8080 -n bigdata

You can get Fernet Key value by running the following:

    echo Fernet Key: $(kubectl get secret --namespace bigdata airflow-fernet-key -o jsonpath="{.data.fernet-key}" | base64 --decode)


# Mount Etme
nohup minikube mount --uid 50000 /home/obase/works/k8s/DAGS:/mnt/airflow-dags > minikube-mount.log 2>&1 &

# Run Airflow with Port Forward
nohup kubectl port-forward --address 0.0.0.0 svc/airflow-api-server 8080:8080 -n bigdata > airflow-ui.log 2>&1 &

# s3 Yeni Bucket Olusturma
 kubectl exec -it $(kubectl get pods -n bigdata -l app=localstack -o jsonpath='{.items[0].metadata.name}') -n bigdata -- awslocal s3 mb s3://data

## S3 e airflow loglarını için bucket oluşturmak
kubectl exec -it $(kubectl get pods -n bigdata -l app=localstack -o jsonpath='{.items[0].metadata.name}') -n bigdata -- awslocal s3 mb s3://airflow-logs

## S3 e airflow loglarını için bucket show

kubectl exec -it $(kubectl get pods -n bigdata -l app=localstack -o jsonpath='{.items[0].metadata.name}') -n bigdata -- awslocal s3 ls s3://airflow-logs/logs/

# Jupyter Port Forwarding
nohup kubectl port-forward --address 0.0.0.0 svc/jupyter-spark-service 8888:8888 -n bigdata > jupyter-ui.log 2>&1 &


# Polaris Port Forwarding
 nohup kubectl port-forward --address 0.0.0.0 svc/polaris-catalog-service 8181:8181 -n bigdata > polaris.log 2>&1 &
 
 #  polaris root cred
kubectl logs -n bigdata -l app=polaris-catalog | grep -a "root principal credentials" | tail -n 1

## Polaris Kalıcı DB Bağlantısı

## Airflow PostgreSQL pod adını bulun
POSTGRES_POD=$(kubectl get pod -n bigdata -l app.kubernetes.io/name=postgresql -o jsonpath="{.items[0].metadata.name}")

## Polaris için ayrı bir db ve kullanıcı açın (veya postgres/airflow kullanıcısını kullanın)
kubectl exec -it -n bigdata $POSTGRES_POD -- psql -U postgres -c "CREATE DATABASE polaris_db;"

- password: postgres


> kubectl port-forward -n bigdata svc/airflow-api-server 8080:8080
> kubectl port-forward -n bigdata svc/jupyter-spark-service 8888:8888
> kubectl port-forward -n bigdata svc/trino 8090:8090