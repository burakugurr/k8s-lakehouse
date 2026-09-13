# LocalStack

## Purpose

Emulates AWS-like S3, STS, and IAM services for local lakehouse development.

## Files

- localstack-deployment.yaml: LocalStack Deployment + Service
- bucketpass.md: local bucket credentials and STS notes

## Deploy

```bash
kubectl apply -n bigdata -f /home/obase/works/k8s/LocalStack/localstack-deployment.yaml
```

## Port-forward

```bash
kubectl port-forward --address 0.0.0.0 svc/localstack-service 4566:4566 -n bigdata
```

Open:

- http://localhost:4566

## Check health

```bash
kubectl exec -n bigdata $(kubectl get pod -n bigdata -l app=localstack -o jsonpath='{.items[0].metadata.name}') -- awslocal sts get-caller-identity
kubectl exec -n bigdata $(kubectl get pod -n bigdata -l app=localstack -o jsonpath='{.items[0].metadata.name}') -- awslocal s3 ls
```

## Bucket credentials

See:

- [bucketpass.md](bucketpass.md)

## Bucket creation

```bash
kubectl exec -n bigdata $(kubectl get pod -n bigdata -l app=localstack -o jsonpath='{.items[0].metadata.name}') -- awslocal s3 mb s3://warehouse
kubectl exec -n bigdata $(kubectl get pod -n bigdata -l app=localstack -o jsonpath='{.items[0].metadata.name}') -- awslocal s3 mb s3://bigdata
kubectl exec -n bigdata $(kubectl get pod -n bigdata -l app=localstack -o jsonpath='{.items[0].metadata.name}') -- awslocal s3 mb s3://airflow-logs
```

## Important notes

- `SERVICES` includes `s3,sts,iam`
- Access keys are intentionally fake and only valid for local emulation
- Use per-bucket credentials and avoid reusing the root keys in app configuration
- For a real environment, replace LocalStack with AWS S3 + IAM and configure proper role policies

## Production hardening

- Replace LocalStack with real S3 and IAM service
- Use bucket policies and IAM roles instead of shared root keys
- Enable TLS and private networking
- Add monitoring and access auditing
