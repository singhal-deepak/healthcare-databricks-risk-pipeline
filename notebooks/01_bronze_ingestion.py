file_path = "/Volumes/my_catalog/dbo/welcome/diabetic_data.csv"


df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

display(df)

spark.sql("DROP TABLE IF EXISTS my_catalog.dbo.bronze_diabetic_raw")

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("my_catalog.dbo.bronze_diabetic_raw")