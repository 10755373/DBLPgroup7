from pyspark.sql import SparkSession


def create_spark_session():
    return (
        SparkSession.builder.master("local")
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )
