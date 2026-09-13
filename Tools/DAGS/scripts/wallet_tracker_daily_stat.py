import argparse
import json
import urllib.request
from datetime import date, datetime
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# Polaris Bağlantı Sabitleri
CLIENT_ID = "66d6a20a79533f2e"
CLIENT_SECRET = "74cdef192dfca9b8cf69763a889464ae"
BASE_URL = "http://polaris-catalog-service.bigdata.svc.cluster.local:8181"
LOCALSTACK_URL = "http://localstack-service.bigdata.svc.cluster.local:4566"


def get_or_create_table(
    spark: SparkSession,
    table_name: str,
    create_sql: str,
    base_url: str,
    client_id: str,
    client_secret: str,
):
    try:
        spark.read.table(table_name).limit(0).collect()
        print(f"✔ Tablo mevcut ve metadata erişilebilir: {table_name}")
    except Exception as e:
        err_text = str(e)
        if any(
            keyword in err_text
            for keyword in [
                "TABLE_OR_VIEW_NOT_FOUND",
                "NoSuchTableException",
                "Location does not exist",
                "NotFoundException",
                "Malformed request",
            ]
        ):
            print(
                f"ℹ️ Tablo bulunamadı veya metadata tutarsız ({table_name}). Sıfırdan oluşturuluyor..."
            )

            try:
                token_req = urllib.request.Request(
                    f"{base_url}/api/catalog/v1/oauth/tokens",
                    data=f"grant_type=client_credentials&client_id={client_id}&client_secret={client_secret}&scope=PRINCIPAL_ROLE:ALL".encode(
                        "utf-8"
                    ),
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    method="POST",
                )
                with urllib.request.urlopen(token_req) as resp:
                    token = json.loads(resp.read().decode())["access_token"]

                tbl_short = table_name.split(".")[-1]
                del_req = urllib.request.Request(
                    f"{base_url}/api/catalog/v1/polaris/namespaces/wallettracker/tables/{tbl_short}?purgeRequested=false",
                    headers={"Authorization": f"Bearer {token}"},
                    method="DELETE",
                )
                urllib.request.urlopen(del_req)
            except Exception:
                pass

            spark.sql(create_sql)
            print(f"✔ Tablo başarıyla oluşturuldu: {table_name}")
        else:
            raise e


def init_spark(target_date_str: str) -> SparkSession:
    print("Spark oturumu başlatılıyor...")
    return (
        SparkSession.builder.appName(f"Polaris-RBAC-User-Session-{target_date_str}")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
            "org.apache.iceberg:iceberg-aws-bundle:1.5.0,"
            "org.postgresql:postgresql:42.7.3",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.polaris.type", "rest")
        .config("spark.sql.catalog.polaris.uri", f"{BASE_URL}/api/catalog")
        .config("spark.sql.catalog.polaris.credential", f"{CLIENT_ID}:{CLIENT_SECRET}")
        .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
        .config("spark.sql.catalog.polaris.warehouse", "polaris")
        .config("spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation", "false")
        .config(
            "spark.sql.catalog.polaris.io-impl",
            "org.apache.iceberg.hadoop.HadoopFileIO",
        )
        .config("spark.sql.catalog.polaris.s3.endpoint", LOCALSTACK_URL)
        .config("spark.sql.catalog.polaris.s3.path-style-access", "true")
        .config("spark.sql.catalog.polaris.s3.access-key-id", "test")
        .config("spark.sql.catalog.polaris.s3.secret-access-key", "test")
        .config("spark.sql.catalog.polaris.client.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.endpoint", LOCALSTACK_URL)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.endpoint", LOCALSTACK_URL)
        .config("spark.hadoop.fs.s3.path.style.access", "true")
        .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.defaultCatalog", "polaris")
        .getOrCreate()
    )


def get_supabase_table(
    spark: SparkSession,
    table_name: str,
    jdbc_url: str,
    db_user: str,
    db_password: str,
):
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", db_user)
        .option("password", db_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def create_iceberg_tables_if_not_exists(spark: SparkSession):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.wallettracker")

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
    ) USING iceberg PARTITIONED BY (days(snapshot_date))
    """
    get_or_create_table(
        spark,
        "polaris.wallettracker.daily_user_asset_holdings",
        create_holdings_sql,
        BASE_URL,
        CLIENT_ID,
        CLIENT_SECRET,
    )

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
    ) USING iceberg PARTITIONED BY (days(snapshot_date))
    """
    get_or_create_table(
        spark,
        "polaris.wallettracker.daily_user_portfolio_stats",
        create_portfolio_stats_sql,
        BASE_URL,
        CLIENT_ID,
        CLIENT_SECRET,
    )

    create_cashflow_sql = """
    CREATE TABLE IF NOT EXISTS polaris.wallettracker.daily_user_cashflow (
        summary_date DATE,
        user_id INT,
        user_name STRING,
        total_income DECIMAL(18, 2),
        total_variable_expense DECIMAL(18, 2),
        total_fixed_expense DECIMAL(18, 2),
        net_cashflow DECIMAL(18, 2),
        savings_rate_pct DECIMAL(5, 2),
        created_at TIMESTAMP
    ) USING iceberg PARTITIONED BY (days(summary_date))
    """
    get_or_create_table(
        spark,
        "polaris.wallettracker.daily_user_cashflow",
        create_cashflow_sql,
        BASE_URL,
        CLIENT_ID,
        CLIENT_SECRET,
    )

    create_budget_performance_sql = """
    CREATE TABLE IF NOT EXISTS polaris.wallettracker.monthly_budget_performance (
        budget_month DATE,
        user_id INT,
        category_id INT,
        category_name STRING,
        budget_amount DECIMAL(18, 2),
        actual_spent DECIMAL(18, 2),
        remaining_amount DECIMAL(18, 2),
        utilization_rate_pct DECIMAL(5, 2),
        is_over_budget BOOLEAN,
        created_at TIMESTAMP
    ) USING iceberg PARTITIONED BY (months(budget_month))
    """
    get_or_create_table(
        spark,
        "polaris.wallettracker.monthly_budget_performance",
        create_budget_performance_sql,
        BASE_URL,
        CLIENT_ID,
        CLIENT_SECRET,
    )

    create_asset_price_summary_sql = """
    CREATE TABLE IF NOT EXISTS polaris.wallettracker.daily_asset_price_summary (
        price_date DATE,
        asset_id INT,
        asset_symbol STRING,
        asset_type STRING,
        open_buy_price DECIMAL(18, 4),
        close_buy_price DECIMAL(18, 4),
        min_buy_price DECIMAL(18, 4),
        max_buy_price DECIMAL(18, 4),
        avg_spread DECIMAL(18, 4),
        sample_points BIGINT,
        created_at TIMESTAMP
    ) USING iceberg PARTITIONED BY (days(price_date))
    """
    get_or_create_table(
        spark,
        "polaris.wallettracker.daily_asset_price_summary",
        create_asset_price_summary_sql,
        BASE_URL,
        CLIENT_ID,
        CLIENT_SECRET,
    )

    create_goal_progress_stats_sql = """
    CREATE TABLE IF NOT EXISTS polaris.wallettracker.daily_goal_progress_stats (
        snapshot_date DATE,
        goal_id INT,
        user_id INT,
        user_name STRING,
        goal_title STRING,
        target_amount DECIMAL(18, 2),
        current_amount DECIMAL(18, 2),
        completion_rate_pct DECIMAL(5, 2),
        deadline DATE,
        days_remaining INT,
        status STRING,
        created_at TIMESTAMP
    ) USING iceberg PARTITIONED BY (days(snapshot_date))
    """
    get_or_create_table(
        spark,
        "polaris.wallettracker.daily_goal_progress_stats",
        create_goal_progress_stats_sql,
        BASE_URL,
        CLIENT_ID,
        CLIENT_SECRET,
    )


def run_etl_pipeline(target_date_str: str = None):
    if not target_date_str:
        target_date_str = str(date.today())
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    target_month_start = target_date.replace(day=1)

    spark = init_spark(target_date_str)
    create_iceberg_tables_if_not_exists(spark)

    DB_HOST = "aws-1-ap-southeast-1.pooler.supabase.com"
    DB_PORT = "5432"
    DB_NAME = "postgres"
    DB_USER = "postgres.jaglvhpsyetfbviuornq"
    DB_PASSWORD = "7Qpdm7IYmHCezTRy"
    jdbc_url = (
        f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
    )

    df_users = get_supabase_table(
        spark, "public.users", jdbc_url, DB_USER, DB_PASSWORD
    )
    df_assets = get_supabase_table(
        spark, "public.assets", jdbc_url, DB_USER, DB_PASSWORD
    )
    df_holdings = get_supabase_table(
        spark, "public.portfolio_holdings", jdbc_url, DB_USER, DB_PASSWORD
    )
    df_summary = get_supabase_table(
        spark, "public.portfolio_summary", jdbc_url, DB_USER, DB_PASSWORD
    )
    df_income = get_supabase_table(
        spark, "public.income_transactions", jdbc_url, DB_USER, DB_PASSWORD
    )
    df_expense = get_supabase_table(
        spark, "public.expense_transactions", jdbc_url, DB_USER, DB_PASSWORD
    )
    df_fixed_pay = get_supabase_table(
        spark, "public.fixed_expense_payments", jdbc_url, DB_USER, DB_PASSWORD
    )
    df_fixed_exp = get_supabase_table(
        spark, "public.fixed_expenses", jdbc_url, DB_USER, DB_PASSWORD
    )
    df_budgets = get_supabase_table(
        spark, "public.budgets", jdbc_url, DB_USER, DB_PASSWORD
    )
    df_exp_cat = get_supabase_table(
        spark, "public.expense_categories", jdbc_url, DB_USER, DB_PASSWORD
    )
    df_price_hist = get_supabase_table(
        spark, "public.price_history", jdbc_url, DB_USER, DB_PASSWORD
    )
    df_goals = get_supabase_table(
        spark, "public.goals", jdbc_url, DB_USER, DB_PASSWORD
    )

    # 1. daily_user_asset_holdings
    holdings_filtered = df_holdings.filter(
        F.to_date(F.col("record_date")) <= F.lit(target_date)
    )
    df_user_asset_holdings = (
        holdings_filtered.join(
            df_users, holdings_filtered["user_id"] == df_users["id"], "inner"
        )
        .join(
            df_assets,
            holdings_filtered["asset_id"] == df_assets["asset_id"],
            "inner",
        )
        .groupBy(
            df_users["id"].alias("user_id"),
            df_users["name"].alias("user_name"),
            df_assets["asset_id"].alias("asset_id"),
            df_assets["asset_symbol"].alias("asset_symbol"),
            df_assets["asset_name"].alias("asset_name"),
            df_assets["asset_type"].alias("asset_type"),
            holdings_filtered["asset_sub_type"].alias("asset_sub_type"),
        )
        .agg(F.sum("quantity").cast("decimal(38, 8)").alias("total_quantity"))
        .withColumn("snapshot_date", F.lit(target_date).cast("date"))
        .withColumn("created_at", F.current_timestamp())
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
            "created_at",
        )
    )
    df_user_asset_holdings.writeTo(
        "polaris.wallettracker.daily_user_asset_holdings"
    ).overwritePartitions()

    # 2. daily_user_portfolio_stats
    summary_filtered = df_summary.filter(
        F.to_date(F.col("timestamp")) == F.lit(target_date)
    )
    df_user_portfolio_stats = (
        summary_filtered.join(
            df_users, summary_filtered["user_id"] == df_users["id"], "inner"
        )
        .groupBy(df_users["id"].alias("user_id"), df_users["name"].alias("user_name"))
        .agg(
            F.avg("total_value")
            .cast("decimal(38, 4)")
            .alias("avg_portfolio_value"),
            F.min("total_value")
            .cast("decimal(38, 4)")
            .alias("min_portfolio_value"),
            F.max("total_value")
            .cast("decimal(38, 4)")
            .alias("max_portfolio_value"),
            F.count("total_value").alias("sample_count"),
        )
        .withColumn("snapshot_date", F.lit(target_date).cast("date"))
        .withColumn("created_at", F.current_timestamp())
        .select(
            "snapshot_date",
            "user_id",
            "user_name",
            "avg_portfolio_value",
            "min_portfolio_value",
            "max_portfolio_value",
            "sample_count",
            "created_at",
        )
    )
    df_user_portfolio_stats.writeTo(
        "polaris.wallettracker.daily_user_portfolio_stats"
    ).overwritePartitions()

    # 3. daily_user_cashflow
    daily_inc = (
        df_income.filter(F.col("date") == F.lit(target_date))
        .groupBy("user_id")
        .agg(F.sum("amount").alias("inc_amt"))
    )
    daily_exp = (
        df_expense.filter(F.col("date") == F.lit(target_date))
        .groupBy("user_id")
        .agg(F.sum("amount").alias("exp_amt"))
    )
    daily_fix = (
        df_fixed_pay.filter(
            F.to_date(F.col("payment_date")) == F.lit(target_date)
        )
        .join(
            df_fixed_exp,
            df_fixed_pay["expense_id"] == df_fixed_exp["id"],
            "inner",
        )
        .groupBy(df_fixed_exp["user_id"].alias("user_id"))
        .agg(F.sum(df_fixed_pay["amount"]).alias("fix_amt"))
    )

    df_daily_cashflow = (
        df_users.select(
            F.col("id").alias("user_id"), F.col("name").alias("user_name")
        )
        .join(daily_inc, "user_id", "left")
        .join(daily_exp, "user_id", "left")
        .join(daily_fix, "user_id", "left")
        .na.fill(0, subset=["inc_amt", "exp_amt", "fix_amt"])
        .filter(
            (F.col("inc_amt") > 0)
            | (F.col("exp_amt") > 0)
            | (F.col("fix_amt") > 0)
        )
        .withColumn("summary_date", F.lit(target_date).cast("date"))
        .withColumn("total_income", F.col("inc_amt").cast("decimal(18, 2)"))
        .withColumn(
            "total_variable_expense", F.col("exp_amt").cast("decimal(18, 2)")
        )
        .withColumn(
            "total_fixed_expense", F.col("fix_amt").cast("decimal(18, 2)")
        )
        .withColumn(
            "net_cashflow",
            (F.col("inc_amt") - (F.col("exp_amt") + F.col("fix_amt"))).cast(
                "decimal(18, 2)"
            ),
        )
        .withColumn(
            "savings_rate_pct",
            F.when(
                F.col("inc_amt") > 0,
                ((F.col("net_cashflow") / F.col("inc_amt")) * 100).cast(
                    "decimal(5, 2)"
                ),
            ).otherwise(F.lit(0.0).cast("decimal(5, 2)")),
        )
        .withColumn("created_at", F.current_timestamp())
        .select(
            "summary_date",
            "user_id",
            "user_name",
            "total_income",
            "total_variable_expense",
            "total_fixed_expense",
            "net_cashflow",
            "savings_rate_pct",
            "created_at",
        )
    )
    df_daily_cashflow.writeTo(
        "polaris.wallettracker.daily_user_cashflow"
    ).overwritePartitions()

    # 4. monthly_budget_performance
    monthly_spent = (
        df_expense.filter(
            (F.col("date") >= F.lit(target_month_start))
            & (F.col("date") <= F.lit(target_date))
        )
        .groupBy("user_id", "category_id")
        .agg(F.sum("amount").alias("actual_spent"))
    )
    budgets_clean = df_budgets.select(
        F.col("user_id"),
        F.col("category_id"),
        F.col("amount").alias("budget_amount"),
    )
    exp_cat_clean = df_exp_cat.select(
        F.col("id").alias("category_id"), F.col("name").alias("category_name")
    )

    df_budget_perf = (
        budgets_clean.join(exp_cat_clean, "category_id", "inner")
        .join(monthly_spent, ["user_id", "category_id"], "left")
        .na.fill(0, subset=["actual_spent"])
        .withColumn("budget_month", F.lit(target_month_start).cast("date"))
        .withColumn("budget_amount", F.col("budget_amount").cast("decimal(18, 2)"))
        .withColumn("actual_spent", F.col("actual_spent").cast("decimal(18, 2)"))
        .withColumn(
            "remaining_amount",
            (F.col("budget_amount") - F.col("actual_spent")).cast(
                "decimal(18, 2)"
            ),
        )
        .withColumn(
            "utilization_rate_pct",
            F.when(
                F.col("budget_amount") > 0,
                (
                    (F.col("actual_spent") / F.col("budget_amount")) * 100
                ).cast("decimal(5, 2)"),
            ).otherwise(F.lit(0.0).cast("decimal(5, 2)")),
        )
        .withColumn(
            "is_over_budget", F.col("actual_spent") > F.col("budget_amount")
        )
        .withColumn("created_at", F.current_timestamp())
        .select(
            "budget_month",
            "user_id",
            "category_id",
            "category_name",
            "budget_amount",
            "actual_spent",
            "remaining_amount",
            "utilization_rate_pct",
            "is_over_budget",
            "created_at",
        )
    )
    df_budget_perf.writeTo(
        "polaris.wallettracker.monthly_budget_performance"
    ).overwritePartitions()

    # 5. daily_asset_price_summary
    df_price_filtered = df_price_hist.filter(
        F.to_date(F.col("timestamp")) == F.lit(target_date)
    )
    window_asc = Window.partitionBy("asset_id").orderBy(
        F.col("timestamp").asc()
    )
    window_desc = Window.partitionBy("asset_id").orderBy(
        F.col("timestamp").desc()
    )

    price_agg = (
        df_price_filtered.withColumn(
            "open_buy", F.first("alis_price").over(window_asc)
        )
        .withColumn("close_buy", F.first("alis_price").over(window_desc))
        .groupBy("asset_id")
        .agg(
            F.first("open_buy").cast("decimal(18, 4)").alias("open_buy_price"),
            F.first("close_buy")
            .cast("decimal(18, 4)")
            .alias("close_buy_price"),
            F.min("alis_price").cast("decimal(18, 4)").alias("min_buy_price"),
            F.max("alis_price").cast("decimal(18, 4)").alias("max_buy_price"),
            F.avg(F.col("satis_price") - F.col("alis_price"))
            .cast("decimal(18, 4)")
            .alias("avg_spread"),
            F.count("timestamp").alias("sample_points"),
        )
    )
    assets_clean = df_assets.select(
        F.col("asset_id"), F.col("asset_symbol"), F.col("asset_type")
    )
    df_price_summary = (
        price_agg.join(assets_clean, "asset_id", "inner")
        .withColumn("price_date", F.lit(target_date).cast("date"))
        .withColumn("created_at", F.current_timestamp())
        .select(
            "price_date",
            "asset_id",
            "asset_symbol",
            "asset_type",
            "open_buy_price",
            "close_buy_price",
            "min_buy_price",
            "max_buy_price",
            "avg_spread",
            "sample_points",
            "created_at",
        )
    )
    df_price_summary.writeTo(
        "polaris.wallettracker.daily_asset_price_summary"
    ).overwritePartitions()

    # 6. daily_goal_progress_stats
    goals_clean = df_goals.select(
        F.col("id").alias("goal_id"),
        F.col("user_id"),
        F.col("title").alias("goal_title"),
        F.col("target_amount"),
        F.col("current_amount"),
        F.col("deadline"),
        F.col("status"),
    )
    df_goal_stats = (
        goals_clean.join(
            df_users.select(
                F.col("id").alias("user_id"), F.col("name").alias("user_name")
            ),
            "user_id",
            "inner",
        )
        .withColumn("snapshot_date", F.lit(target_date).cast("date"))
        .withColumn("target_amount", F.col("target_amount").cast("decimal(18, 2)"))
        .withColumn(
            "current_amount",
            F.coalesce(F.col("current_amount"), F.lit(0)).cast("decimal(18, 2)"),
        )
        .withColumn(
            "completion_rate_pct",
            F.when(
                F.col("target_amount") > 0,
                (
                    (F.col("current_amount") / F.col("target_amount")) * 100
                ).cast("decimal(5, 2)"),
            ).otherwise(F.lit(0.0).cast("decimal(5, 2)")),
        )
        .withColumn("deadline", F.col("deadline").cast("date"))
        .withColumn(
            "days_remaining", F.datediff(F.col("deadline"), F.lit(target_date))
        )
        .withColumn(
            "status",
            F.when(
                F.col("current_amount") >= F.col("target_amount"),
                F.lit("completed"),
            )
            .when(F.col("days_remaining") < 0, F.lit("behind_schedule"))
            .otherwise(F.col("status")),
        )
        .withColumn("created_at", F.current_timestamp())
        .select(
            "snapshot_date",
            "goal_id",
            "user_id",
            "user_name",
            "goal_title",
            "target_amount",
            "current_amount",
            "completion_rate_pct",
            "deadline",
            "days_remaining",
            "status",
            "created_at",
        )
    )
    df_goal_stats.writeTo(
        "polaris.wallettracker.daily_goal_progress_stats"
    ).overwritePartitions()

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="WalletTracker Analytics Pipeline"
    )
    parser.add_argument(
        "--target-date",
        dest="target_date",
        type=str,
        default=str(date.today()),
    )
    args, _ = parser.parse_known_args()
    run_etl_pipeline(args.target_date)