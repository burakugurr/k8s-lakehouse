# DAGs

## Purpose

This directory contains Python DAGs used by Airflow.

## Current DAGs

- test_dag.py: basic demo DAG
- lists3.py: Lists S3 buckets and objects through the LocalStack AWS connection

## Local operation

These files are expected to be visible from the Airflow scheduler.

```bash
ls -la /home/obase/works/k8s/DAGS
```

## Refresh behavior

When files change on the host machine, the scheduler needs a reload.

Typical recovery steps:

```bash
kubectl delete pod -n bigdata -l component=scheduler --grace-period=0 --force
kubectl wait --for=condition=Ready pod -l component=scheduler -n bigdata --timeout=180s
```

## Important notes

- Keep DAG file names unique
- Avoid creating imports that depend on local-only workdirs
- Prefer explicit imports and stable Airflow decorators
- Keep task IDs deterministic for debugging

## Production hardening

- Move DAG source to Git sync or internal GitOps repository
- Use CI to lint DAG files before deployment
- Validate DAG parse correctness before rollout
- Add snapshot / backup policy for DAG volumes
