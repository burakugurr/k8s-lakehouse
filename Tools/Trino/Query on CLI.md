TRINO_POD=$(kubectl get pods -n bigdata -l app.kubernetes.io/component=coordinator --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')

# --server localhost:8090 parametresi ile bağlanın:
kubectl exec -it $TRINO_POD -n bigdata -- trino --server localhost:8090 --catalog iceberg


SHOW SCHEMAS;
SHOW TABLES FROM wallettracker;
SELECT * FROM wallettracker.daily_asset_price_summary LIMIT 10;