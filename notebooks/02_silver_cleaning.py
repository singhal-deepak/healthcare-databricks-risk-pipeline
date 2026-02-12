# SILVER LAYER — Healthcare Data Cleaning

from pyspark.sql.functions import col, when

# Read from Bronze table
df = spark.table("my_catalog.dbo.bronze_diabetic_raw")

print("Bronze Row Count:", df.count())

# ---------------------------
# 1️⃣ Remove Duplicate Encounters
# ---------------------------
df = df.dropDuplicates(["encounter_id"])

# ---------------------------
# 2️⃣ Handle Missing Values
# ---------------------------
df = df.fillna({
    "race": "Unknown",
    "diag_1": "Unknown",
    "diag_2": "Unknown",
    "diag_3": "Unknown"
})

# ---------------------------
# 3️⃣ Convert Readmission → Numeric Flag
# ---------------------------
df = df.withColumn(
    "readmitted_flag",
    when(col("readmitted") == "NO", 0).otherwise(1)
)

# ---------------------------
# 4️⃣ Age Bucket Feature
# ---------------------------
df = df.withColumn(
    "age_group",
    when(col("age").contains("0-10"), 0)
    .when(col("age").contains("10-20"), 1)
    .when(col("age").contains("20-30"), 2)
    .when(col("age").contains("30-40"), 3)
    .when(col("age").contains("40-50"), 4)
    .when(col("age").contains("50-60"), 5)
    .when(col("age").contains("60-70"), 6)
    .when(col("age").contains("70-80"), 7)
    .when(col("age").contains("80-90"), 8)
    .otherwise(9)
)

# ---------------------------
# 5️⃣ Length of Stay Category
# ---------------------------
df = df.withColumn(
    "los_bucket",
    when(col("time_in_hospital") <= 3, "Short")
    .when(col("time_in_hospital") <= 7, "Medium")
    .otherwise("Long")
)

print("Silver Ready Row Count:", df.count())

display(df)

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("my_catalog.dbo.silver_diabetic_clean")