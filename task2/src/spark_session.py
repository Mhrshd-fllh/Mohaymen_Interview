import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(dotenv_path=env_path)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadminpassword")
SPARK_HADOOP_AWS_PACKAGE = os.getenv("SPARK_HADOOP_AWS_PACKAGE", "org.apache.hadoop:hadoop-aws:3.3.4")
AWS_JAVA_SDK_PACKAGE = os.getenv("AWS_JAVA_SDK_PACKAGE", "com.amazonaws:aws-java-sdk-bundle:1.12.262")


def get_spark_session(app_name: str = "Task2_Spark_MinIO_Pipeline") -> SparkSession:
    maven_packages = f"{SPARK_HADOOP_AWS_PACKAGE},{AWS_JAVA_SDK_PACKAGE}"

    builder: SparkSession.Builder = (
        SparkSession.Builder()
        .appName(app_name)
        .config("spark.jars.packages", maven_packages)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark