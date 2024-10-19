from pyspark.sql import SparkSession

def main():
    # Create a SparkSession with Hadoop S3A configurations
    spark = SparkSession.builder \
        .appName("PySparkBronzeRawData") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    # Read data from MinIO (replace 'your-bucket' and 'your-data.csv' accordingly)
    df = (
        spark.read.csv("s3a://bronze-raw-data/test/report-52112288000190-2024_06_28.csv", header=True, inferSchema=True, sep="|")
        )
    df.printSchema()
    df.show()

    spark.stop()

if __name__ == "__main__":
    print('Running PySparkBronzeRawData...')
    main()