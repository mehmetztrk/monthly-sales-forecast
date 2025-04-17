from pyspark.sql import SparkSession

# 1. Spark oturumu başlat
spark = SparkSession.builder \
    .appName("CRM Sales Analysis") \
    .getOrCreate()

# 2. Dosyaları oku
accounts_df = spark.read.csv("data/accounts.csv", header=True, inferSchema=True)
products_df = spark.read.csv("data/products.csv", header=True, inferSchema=True)
sales_pipeline_df = spark.read.csv("data/sales_pipeline.csv", header=True, inferSchema=True)
sales_teams_df = spark.read.csv("data/sales_teams.csv", header=True, inferSchema=True)

# 3. Verileri göster
print("Accounts:")
accounts_df.show(5)
print("Products:")
products_df.show(5)
print("Sales Pipeline:")
sales_pipeline_df.show(5)
print("Sales Teams:")
sales_teams_df.show(5)

print(sales_pipeline_df.columns)
print(products_df.columns)
