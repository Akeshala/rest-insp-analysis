from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

data_url = 's3://emr-project/input/DelayedFlights.csv'
output_url = 's3://emr-project/output/'

def transform_data() -> None:
    try:
        spark = SparkSession.builder.appName("FirstApp").getOrCreate()
        logger.info("Spark session created")

        # Read CSV file from S3
        df = spark.read.option("header", "true").csv(data_url)
        logger.info("Data read from S3")

        df.createOrReplaceTempView("data_set")

        # SQL query to group data
        GROUP_BY_QUERY = """
            SELECT CRSArrTime, count(*) AS total_count
            FROM data_set
            WHERE UniqueCarrier = 'WN'
            GROUP BY CRSArrTime
        """

        transformed_df = spark.sql(GROUP_BY_QUERY)
        logger.info("Data transformed")

        # Print count for debugging purposes
        logger.info(f"Transformed Data Count: {transformed_df.count()}")

        # Write the transformed data to S3 in Parquet format
        transformed_df.write.mode("overwrite").parquet(output_url)
        logger.info("Data written to S3")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    transform_data()
