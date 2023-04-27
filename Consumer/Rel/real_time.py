import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, DoubleType, BooleanType, DateType
from pyspark.sql.functions import col, from_json, sum


movies_schema = StructType([
    StructField("poster_path", StringType(), True),
    StructField("adult", BooleanType(), True),
    StructField("overview", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("genre_ids", ArrayType(IntegerType()), True),
    StructField("id", IntegerType(), True),
    StructField("original_title", StringType(), True),
    StructField("original_language", StringType(), True),
    StructField("title", StringType(), True),
    StructField("backdrop_path", StringType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("vote_count", IntegerType(), True),
    StructField("video", BooleanType(), True),
    StructField("vote_average", DoubleType(), True)
])

def write_df(df, epoch_id,tablename ):
    print(f"epoch id: {epoch_id}")
    PSQL_SERVERNAME= "postgres"
    PSQL_PORTNUMBER = 5432
    PSQL_DBNAME = "postgres"
    PSQL_USERNAME = "postgres"
    PSQL_PASSWORD = "postgres"
    URL = f"jdbc:postgresql://{PSQL_SERVERNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"
    try:
        df.write.format("jdbc").options(
        url=URL,
        driver="org.postgresql.Driver",
        user=PSQL_USERNAME,
        password=PSQL_PASSWORD,
        dbtable=tablename
        ).mode("overwrite").save()
    except Exception as e:
        print(f"Error saving DataFrame to database:{e}")

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

if __name__ == '__main__':

    HDFS_NAMENODE = "hdfs://namenode:9000"
    TOPIC = "tmdb-movies"
    KAFKA_BROKER = "kafka1:19092"

    spark = SparkSession\
        .builder\
        .appName("StreamingProcessing")\
        .getOrCreate()
    quiet_logs(spark)

    df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", TOPIC) \
            .load()

    df= df.withColumn("value", col("value").cast("string"))\
        .withColumn("parsed_value", from_json(col("value"), movies_schema))
    #vote_count
    df_filtered = df.select(
    col("key").cast("string"),
    col("parsed_value.*"),
    col("parsed_value.vote_count").cast("double").alias("vote_count_cast")

    ).filter(
        col("parsed_value.vote_count") < 1000
    )

    # console_output = df_filtered \
    #     .writeStream \
    #     .format("console") \
    #     .outputMode("append") \
    #     .option("truncate", "false") \
    #     .start()

    # console_output.awaitTermination()


    #df_filtered_agg = df_filtered.groupBy("key").agg(sum("parsed_value.vote_count").alias("total_vote_count"))
    df_filtered_agg = df_filtered.groupBy("key").agg(sum(col("vote_count_cast")).alias("total_vote_count"))
    dstreaming_query = df_filtered_agg.writeStream \
        .outputMode("complete") \
        .foreachBatch(lambda df, epoch_id: write_df(df, epoch_id, "movies_total_ratings")) \
        .start()

    dstreaming_query.awaitTermination()