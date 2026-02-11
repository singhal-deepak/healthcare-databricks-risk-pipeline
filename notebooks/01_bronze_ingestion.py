# Databricks Bronze Layer Ingestion

df = spark.read.csv(
    "/FileStore/tables/patient_data.csv",
    header=True,
    inferSchema=True
)

df.write.format("delta").mode("overwrite").saveAsTable("bronze_patient_raw")

print("Bronze table created successfully")
