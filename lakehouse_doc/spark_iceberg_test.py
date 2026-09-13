from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# S3 (MinIO) ve Iceberg konfigürasyonları ile SparkSession başlat
spark = SparkSession.builder \
    .appName("Lakehouse-Iceberg-Init") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakehouse.type", "hadoop") \
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse-data/warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:30000") \
    .config("spark.hadoop.fs.s3a.access.key", "lakehouse_admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "lakehouse_password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Örnek Şema (Oracle-CDC'deki INVENTORY_ITEMS tablosuna benzer)
schema = StructType([
    StructField("ITEM_NAME", StringType(), False),
    StructField("QUANTITY", IntegerType(), False)
])

# Örnek Veri
data = [
    ("Laptop", 50),
    ("Monitor", 150),
    ("Mouse", 500)
]

df = spark.createDataFrame(data, schema)

# Veriyi Iceberg formatında MinIO'ya yaz
df.write \
    .format("iceberg") \
    .mode("overwrite") \
    .save("lakehouse.default.inventory_items")

print("Veri başarıyla Lakehouse'a (MinIO -> Iceberg) yazıldı!")
spark.stop()