from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Create Company Dimension")
        .config("spark.sql.catalog.nessie.ref", "main")
        .getOrCreate()
    )

    df_jobs = spark.read.table("nessie.silver.jobs")

    df_company = (
        df_jobs
        .select(F.col("company_clean").alias("company_name"))
        .filter(F.col("company_name").isNotNull() & (F.col("company_name") != ""))
        .dropDuplicates(["company_name"])
        .withColumn("company_key", F.sha2(F.col("company_name"), 256))
        .select("company_key", "company_name")
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    df_company.write.format("iceberg").mode("overwrite").saveAsTable("nessie.gold.dim_company")
