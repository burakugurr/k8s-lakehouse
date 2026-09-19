import argparse
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import urllib.request
import json
from pyspark.errors.exceptions.captured import AnalysisException

def get_or_create_table(spark: SparkSession, table_name: str, create_sql: str, base_url: str, client_id: str, client_secret: str):
    try:
        spark.read.table(table_name).limit(0).collect()
        print(f"✔ Tablo mevcut ve metadata erişilebilir: {table_name}")
    except Exception as e:
        err_text = str(e)
        # Tablo hiç yoksa VEYA metadata tutarsızlığı varsa yakala
        if any(keyword in err_text for keyword in [
            "TABLE_OR_VIEW_NOT_FOUND",
            "NoSuchTableException",
            "Location does not exist",
            "NotFoundException",
            "Malformed request"
        ]):
            print(f"ℹ️ Tablo bulunamadı veya metadata tutarsız ({table_name}). Sıfırdan oluşturuluyor...")
            
            # Eğer eski bir artık kayıt varsa Polaris REST API üzerinden temizlemeyi dene
            try:
                token_req = urllib.request.Request(
                    f"{base_url}/api/catalog/v1/oauth/tokens",
                    data=f"grant_type=client_credentials&client_id={client_id}&client_secret={client_secret}&scope=PRINCIPAL_ROLE:ALL".encode("utf-8"),
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    method="POST"
                )
                with urllib.request.urlopen(token_req) as resp:
                    token = json.loads(resp.read().decode())["access_token"]
                
                tbl_short = table_name.split(".")[-1]
                del_req = urllib.request.Request(
                    f"{base_url}/api/catalog/v1/polaris/namespaces/wallettracker/tables/{tbl_short}?purgeRequested=false",
                    headers={"Authorization": f"Bearer {token}"},
                    method="DELETE"
                )
                urllib.request.urlopen(del_req)
            except Exception:
                pass  # Tablo zaten yoksa API hatasını sessizce geç

            # Tabloyu sıfırdan oluştur
            spark.sql(create_sql)
            print(f"✔ Tablo başarıyla oluşturuldu: {table_name}")
        else:
            raise e

def get_supabase_table(spark: SparkSession, table_name: str, jdbc_url: str, db_user: str, db_password: str):
    return spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", "org.postgresql.Driver") \
        .load()

def main(target_date_str: str = None):
    if not target_date_str:
        target_date_str = str(date.today())
    
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    print(f"İşlem başlatılıyor - Hedef Tarih: {target_date}")

    # --- BURAK USER ---
    CLIENT_ID = "66d6a20a79533f2e"
    CLIENT_SECRET = "74cdef192dfca9b8cf69763a889464ae"
    
    base_url = "http://polaris-catalog-service.bigdata.svc.cluster.local:8181"
    localstack_url = "http://localstack-service.bigdata.svc.cluster.local:4566"

    print("Spark oturumu başlatılıyor...")
    spark = (
        SparkSession.builder.appName(f"Polaris-RBAC-User-Session-{target_date_str}")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
            "org.apache.iceberg:iceberg-aws-bundle:1.5.0,"
            "org.postgresql:postgresql:42.7.3"
        )
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        
        # Polaris REST Catalog Tanımlamaları
        .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.polaris.type", "rest")
        .config("spark.sql.catalog.polaris.uri", f"{base_url}/api/catalog")
        .config("spark.sql.catalog.polaris.credential", f"{CLIENT_ID}:{CLIENT_SECRET}")
        .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
        .config("spark.sql.catalog.polaris.warehouse", "polaris")
        .config("spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation", "false")
        .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        
        # S3 / REST Catalog Endpoint ve Path-Style Yönlendirmesi
        .config("spark.sql.catalog.polaris.s3.endpoint", localstack_url)
        .config("spark.sql.catalog.polaris.s3.path-style-access", "true")
        .config("spark.sql.catalog.polaris.s3.access-key-id", "test")
        .config("spark.sql.catalog.polaris.s3.secret-access-key", "test")
        .config("spark.sql.catalog.polaris.client.region", "us-east-1")
        
        # Hadoop fs.s3a Yapılandırması
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.endpoint", localstack_url)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        
        # Hadoop fs.s3 ve s3n şemalarını doğrudan s3a'ya eşleme
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.endpoint", localstack_url)
        .config("spark.hadoop.fs.s3.path.style.access", "true")
        .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        .config("spark.sql.defaultCatalog", "polaris")
        .getOrCreate()
    )

    DB_HOST = "aws-1-ap-southeast-1.pooler.supabase.com"
    DB_PORT = "5432"
    DB_NAME = "postgres"
    DB_USER = "postgres.jaglvhpsyetfbviuornq"
    DB_PASSWORD = "7Qpdm7IYmHCezTRy"
    jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
    
    # Supabase Veri Okuma İşlemleri
    df_users = get_supabase_table(spark, "public.users", jdbc_url, DB_USER, DB_PASSWORD)
    df_assets = get_supabase_table(spark, "public.assets", jdbc_url, DB_USER, DB_PASSWORD)
    df_holdings = get_supabase_table(spark, "public.portfolio_holdings", jdbc_url, DB_USER, DB_PASSWORD)
    df_summary = get_supabase_table(spark, "public.portfolio_summary", jdbc_url, DB_USER, DB_PASSWORD)
    
    # -------------------------------------------------------------
    # 1. POLARIS NAMESPACE & TABLO HAZIRLIKLARI (FAIL-SAFE)
    # -------------------------------------------------------------
    #spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.wallettracker")
    
    create_holdings_sql = """
    CREATE TABLE IF NOT EXISTS polaris.wallettracker.daily_user_asset_holdings (
        snapshot_date DATE,
        user_id INT,
        user_name STRING,
        asset_id INT,
        asset_symbol STRING,
        asset_name STRING,
        asset_type STRING,
        asset_sub_type STRING,
        total_quantity DECIMAL(38, 8),
        created_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (days(snapshot_date))
    """
    get_or_create_table(spark, "polaris.wallettracker.daily_user_asset_holdings", create_holdings_sql, base_url, CLIENT_ID, CLIENT_SECRET)

    create_portfolio_stats_sql = """
    CREATE TABLE IF NOT EXISTS polaris.wallettracker.daily_user_portfolio_stats (
        snapshot_date DATE,
        user_id INT,
        user_name STRING,
        avg_portfolio_value DECIMAL(38, 4),
        min_portfolio_value DECIMAL(38, 4),
        max_portfolio_value DECIMAL(38, 4),
        sample_count BIGINT,
        created_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (days(snapshot_date))
    """
    get_or_create_table(spark, "polaris.wallettracker.daily_user_portfolio_stats", create_portfolio_stats_sql, base_url, CLIENT_ID, CLIENT_SECRET)
    
    # -------------------------------------------------------------
    # 1. TABLO: Kullanıcı & Varlık Bazlı Günlük Miktarlar
    # -------------------------------------------------------------
    holdings_filtered = df_holdings.filter(
        F.to_date(F.col("record_date")) <= F.lit(target_date)
    )

    df_user_asset_holdings = holdings_filtered \
        .join(df_users, holdings_filtered["user_id"] == df_users["id"], "inner") \
        .join(df_assets, holdings_filtered["asset_id"] == df_assets["asset_id"], "inner") \
        .groupBy(
            df_users["id"].alias("user_id"),
            df_users["name"].alias("user_name"),
            df_assets["asset_id"].alias("asset_id"),
            df_assets["asset_symbol"].alias("asset_symbol"),
            df_assets["asset_name"].alias("asset_name"),
            df_assets["asset_type"].alias("asset_type"),
            holdings_filtered["asset_sub_type"].alias("asset_sub_type")
        ) \
        .agg(F.sum("quantity").cast("decimal(38, 8)").alias("total_quantity")) \
        .withColumn("snapshot_date", F.lit(target_date).cast("date")) \
        .withColumn("created_at", F.current_timestamp()) \
        .select(
            "snapshot_date",
            "user_id",
            "user_name",
            "asset_id",
            "asset_symbol",
            "asset_name",
            "asset_type",
            "asset_sub_type",
            "total_quantity",
            "created_at"
        )

    # DÜZELTME: overwritePartitions() yerine append() kullanıldı.
    df_user_asset_holdings.writeTo("polaris.wallettracker.daily_user_asset_holdings") \
        .append()

    print("Tablo 1 (daily_user_asset_holdings) başarıyla log olarak eklendi.")

    # -------------------------------------------------------------
    # 2. TABLO: Kullanıcı Bazlı Günlük Ortalama Portföy Değeri
    # -------------------------------------------------------------
    summary_filtered = df_summary.filter(
        F.to_date(F.col("timestamp")) == F.lit(target_date)
    )

    df_user_portfolio_stats = summary_filtered \
        .join(df_users, summary_filtered["user_id"] == df_users["id"], "inner") \
        .groupBy(
            df_users["id"].alias("user_id"),
            df_users["name"].alias("user_name")
        ) \
        .agg(
            F.avg("total_value").cast("decimal(38, 4)").alias("avg_portfolio_value"),
            F.min("total_value").cast("decimal(38, 4)").alias("min_portfolio_value"),
            F.max("total_value").cast("decimal(38, 4)").alias("max_portfolio_value"),
            F.count("total_value").alias("sample_count")
        ) \
        .withColumn("snapshot_date", F.lit(target_date).cast("date")) \
        .withColumn("created_at", F.current_timestamp()) \
        .select(
            "snapshot_date",
            "user_id",
            "user_name",
            "avg_portfolio_value",
            "min_portfolio_value",
            "max_portfolio_value",
            "sample_count",
            "created_at"
        )

    # DÜZELTME: overwritePartitions() yerine append() kullanıldı.
    df_user_portfolio_stats.writeTo("polaris.wallettracker.daily_user_portfolio_stats") \
        .append()

    print("Tablo 2 (daily_user_portfolio_stats) başarıyla log olarak eklendi.")
    print(f"GÜN:{target_date} İşlem başarıyla tamamlandı.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Daily Wallet Tracker Job")
    parser.add_argument(
        "--target-date", 
        type=str, 
        help="Target date for the job in YYYY-MM-DD format"
    )
    args = parser.parse_args()
    
    main(target_date_str=args.target_date)