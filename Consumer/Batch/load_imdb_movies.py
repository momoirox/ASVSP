import requests
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, DoubleType, BooleanType, DateType
from pyspark.sql.functions import col

# create a SparkSession
spark = SparkSession.builder.appName("IMDbDataSet").getOrCreate()

YOUR_API_KEY = os.environ["YOUR_API_KEY"]
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]


movie_data = spark.read.csv(HDFS_NAMENODE + "/raw/movies.csv", header=True)

# Define the struct type for movies
movie_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("certificate", StringType(), True),
    StructField("duration", IntegerType(), True),
    StructField("genre", ArrayType(StringType()), True),
    StructField("votes", IntegerType(), True),
    StructField("gross_income", DoubleType(), True),
    StructField("directors_id", ArrayType(IntegerType()), True)
])

# Convert the "vote_average" field to float
for movie in movie_data:
    movie["vote_average"] = float(movie["vote_average"])
    movie["popularity"] = float(movie["popularity"])

# create a DataFrame from the movie data
movie_df = spark.createDataFrame(movie_data, schema=movies_schema)


# fetch genre for each movie and combine them with the movie data

genre_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

genre_data = []
genre_path = "genre/movie/list"
genre_url =  movie_url = "{0}{1}?api_key={2}&language={3}".format(base_url, genre_path, api_key, language)
genre_response = requests.get(genre_url)


if genre_response.status_code == 200:
        genre_data = genre_response.json()["genres"]
else:
    print(f"Failed to retrieve {genre_response.text}")

genre_df = spark.createDataFrame(genre_data,genre_schema)

  
#  "movie_df" is a DataFrame with the "movie" schema
#  "genre_df" is a DataFrame with the "genre_schema" schema
genre_df = genre_df.selectExpr("id as join_genre_id", "name as genre_name")
# Explode the "genre_ids" array column into separate rows
exploded_movie_df = movie_df.selectExpr("*", "explode(genre_ids) as genre_id")

# Join the exploded movie DataFrame with the genre DataFrame on the "id" column
joined_df = exploded_movie_df.join(genre_df, exploded_movie_df.genre_id == genre_df.join_genre_id, "left_outer")

# Select the desired columns and drop the temporary "genre_id" column
final_df = joined_df.select(
    col("id").alias("movie_id"), 
    col("overview").alias("overview"), 
    col("popularity").alias("popularity"), 
    col("vote_average").alias("vote_average"), 
    col("vote_count").alias("vote_count"), 
    col("video").alias("video"), 
    col("original_title").alias("original_title"), 
    col("release_date").alias("release_date"), 
    col("adult").alias("adult"), 
    col("original_language").alias("original_language"), 
    col("backdrop_path").alias("backdrop_path"), 
    col("title").alias("title"), 
    col("poster_path").alias("poster_path"), 
    col("genre_name").alias("genre_name")
).drop("join_genre_id")


final_df.write.mode("overwrite").csv(HDFS_NAMENODE + "/raw/movies_and_genres.csv", header=True)

# stop the SparkSession
spark.stop()

