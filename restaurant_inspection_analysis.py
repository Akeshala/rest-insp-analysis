from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import when
from pyspark.sql.functions import min as spark_min, max as spark_max

spark = SparkSession.builder.appName("FirstApp").getOrCreate()

# Read all CSV files into a single DataFrame
df1 = spark.read.csv("s3://emr-project/output/dataset1/" + "*.csv", header=True, inferSchema=True)

# Read all CSV files into a single DataFrame
df2 = spark.read.csv("s3://emr-project/output/dataset2/" + "*.csv", header=True, inferSchema=True)

# Read all CSV files into a single DataFrame
df3 = spark.read.csv("s3://emr-project/output/dataset3/" + "*.csv", header=True, inferSchema=True)

df3.count()

df3.show()

# Remove rows where the score column is NULL
df3 = df3.dropna(subset=["score"])
# df3 = df3.filter(col("score") != "NULL")

df3.count()

df3.show()

min_score = df3.select(spark_min(col("score"))).first()[0]
max_score = df3.select(spark_max(col("score"))).first()[0]


df3_normalized = df3.withColumn("normalized_score", (col("score") - min_score) / (max_score - min_score))

df3_normalized.show()


