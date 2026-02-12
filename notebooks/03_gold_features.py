from pyspark.sql.functions import sum, count

df = spark.read.table("my_catalog.dbo.silver_diabetic_clean")

gold_df = (
    df.groupBy("patient_nbr")
    .agg(
        count("encounter_id").alias("total_encounters"),
        sum("num_medications").alias("total_meds"),
        sum("number_inpatient").alias("total_inpatient_visits"),
        sum("time_in_hospital").alias("total_hospital_days"),
        sum("readmitted_flag").alias("readmit_count")
    )
)

gold_df.write.mode("overwrite").saveAsTable("my_catalog.dbo.gold_patient_features")
