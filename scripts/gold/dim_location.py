from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Create Location Dimension")
        .config("spark.sql.catalog.nessie.ref", "main")
        .getOrCreate()
    )

    df_jobs = spark.read.table("nessie.silver.jobs")

    df_loc = (
        df_jobs
        .select(F.col("location_clean").alias("location_name"))
        .filter(F.col("location_name").isNotNull() & (F.col("location_name") != ""))
        .dropDuplicates(["location_name"])
        .withColumn("location_key", F.sha2(F.col("location_name"), 256))
        .select("location_key", "location_name")
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    df_loc.write.format("iceberg").mode("overwrite").saveAsTable("nessie.gold.dim_location")
