from pyspark.sql import SparkSession
from join import get_enriched_sales
from pyspark.sql.functions import col
from pyspark.sql.functions import to_date, date_format
# Spark session
spark = SparkSession.builder \
    .appName("CRM Sales Analysis - Insights") \
    .getOrCreate()

# Enriched DataFrame'i al
enriched_sales = get_enriched_sales()

# 🎯 ANALİZ 1: Sektöre göre toplam kazanılan fırsat değeri ("Closed Won")
closed_won_df = enriched_sales.filter(enriched_sales["deal_stage"] == "Won")

sector_summary = closed_won_df.groupBy("sector") \
    .sum("close_value") \
    .withColumnRenamed("sum(close_value)", "total_closed_won_value") \
    .orderBy("total_closed_won_value", ascending=False)

print("🔎 Total Revenue by Sector (Closed Won Deals):")
sector_summary.show(truncate=False)

# 🎯 ANALİZ 2: En çok gelir getiren ürünler
product_summary = closed_won_df.groupBy("product") \
    .sum("close_value") \
    .withColumnRenamed("sum(close_value)", "total_closed_won_value") \
    .orderBy("total_closed_won_value", ascending=False)

print("🔎 Total Revenue by Product (Closed Won Deals):")
product_summary.show(truncate=False)

# 🎯 ANALİZ 3: Satış temsilcilerinin kapattığı fırsat toplamı
agent_summary = closed_won_df.groupBy("sales_agent") \
    .sum("close_value") \
    .withColumnRenamed("sum(close_value)", "total_closed_won_value") \
    .orderBy("total_closed_won_value", ascending=False)

print("🔎 Revenue Closed by Sales Agent:")
agent_summary.show(truncate=False)



engaging_df = enriched_sales.filter(col("deal_stage") == "Engaging")

engaging_by_sector = engaging_df.groupBy("sector") \
    .count() \
    .withColumnRenamed("count", "engaging_stage_count") \
    .orderBy("engaging_stage_count", ascending=False)

print("📌 Engaging Stage - Opportunities by Sector:")
engaging_by_sector.show(truncate=False)

enriched_sales.groupBy("deal_stage", "sector") \
    .count() \
    .orderBy("deal_stage", "count", ascending=False) \
    .show(truncate=False)



# Sadece Closed Won fırsatlar
closed_won_df = enriched_sales.filter(enriched_sales["deal_stage"] == "Won")

# Tarihi düzgün formata çevir
closed_won_df = closed_won_df.withColumn("close_date", to_date("close_date", "yyyy-MM-dd"))

# Yıl + Ay sütunu oluştur
monthly_sales = closed_won_df.withColumn("year_month", date_format("close_date", "yyyy-MM"))

# Aylık toplam satış
monthly_sales = monthly_sales.groupBy("year_month") \
    .sum("close_value") \
    .withColumnRenamed("sum(close_value)", "monthly_total") \
    .orderBy("year_month")

# Pandas'a çevirip CSV olarak kaydet
monthly_sales.toPandas().to_csv("data/monthly_sales.csv", index=False)
