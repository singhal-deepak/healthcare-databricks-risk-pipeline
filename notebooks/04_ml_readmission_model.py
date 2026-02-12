from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression

df = spark.read.table("my_catalog.dbo.gold_patient_features")

assembler = VectorAssembler(
    inputCols=[
        "total_encounters",
        "total_meds",
        "total_inpatient_visits",
        "total_hospital_days"
    ],
    outputCol="features"
)

ml_df = assembler.transform(df).select("features", "readmit_count")

lr = LogisticRegression(
    labelCol="readmit_count",
    featuresCol="features"
)

model = lr.fit(ml_df)

print("Accuracy:", model.summary.accuracy)
