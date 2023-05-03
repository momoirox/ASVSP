# This file uses TMDB Api to load data
# it is not used in the project, instead it is
# just an experiment to see how to use TMDB Api

import requests
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, DoubleType, BooleanType, DateType
from pyspark.sql.functions import col

# create a SparkSession
spark = SparkSession.builder.appName("MoviesAndReviews").getOrCreate()

YOUR_API_KEY = os.environ["YOUR_API_KEY"]
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]


pages = range(1, 125)  # get pages 1 to 125
movie_data = []
# set up TMDb API parameters
api_key = YOUR_API_KEY
base_url = "https://api.themoviedb.org/3/"
movie_path = "movie/top_rated"

language = "en-US"

for page in pages:
    movie_url = "{0}{1}?api_key={2}&language={3}&page={4}".format(base_url, movie_path, api_key, language, page)
    movie_response = requests.get(movie_url)
    if movie_response.status_code == 200:
        data = movie_response.json()
        movie_data.extend(data["results"])
    else:
        print(f"Failed to retrieve page {page}: {movie_response.text}")


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

