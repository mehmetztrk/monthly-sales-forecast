from pyspark.sql import SparkSession

# Spark başlat
spark = SparkSession.builder \
    .appName("CRM Sales Analysis") \
    .getOrCreate()

# Verileri oku
sales_pipeline_df = spark.read.csv("data/sales_pipeline.csv", header=True, inferSchema=True)
accounts_df = spark.read.csv("data/accounts.csv", header=True, inferSchema=True)
products_df = spark.read.csv("data/products.csv", header=True, inferSchema=True)

# 1. sales_pipeline + accounts
sales_with_accounts = sales_pipeline_df.join(accounts_df, on="account", how="left")

# 2. Ürün bilgilerini de ekle
enriched_sales = sales_with_accounts.join(products_df, on="product", how="left")

# Kontrol: birleşmiş tabloyu görelim
enriched_sales.select(
    "opportunity_id",
    "sales_agent",
    "account",
    "sector",
    "product",
    "sales_price",
    "deal_stage",
    "close_value",
    "office_location"
).show(10, truncate=False)

def get_enriched_sales():
    return enriched_sales
