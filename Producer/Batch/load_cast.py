import requests
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, FloatType, StructField, IntegerType, StringType, ArrayType, BooleanType, DateType
from pyspark.sql.functions import collect_list

# create a SparkSession
spark = SparkSession.builder.appName("ReadMovieIds").getOrCreate()

YOUR_API_KEY = os.environ["YOUR_API_KEY"]
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]



movie_genres_df = spark.read.csv(HDFS_NAMENODE + "/raw/movies_and_genres.csv", header=True)
movie_ids = [row["movie_id"] for row in movie_genres_df.select("movie_id").distinct().orderBy("movie_id").collect()]
movie_id_strings = list(map(str, movie_ids))

cast_data = []
MAX_ATTEMPTS = 3

api_key = YOUR_API_KEY
base_url = "https://api.themoviedb.org/3/movie/"


language = "en-US"

for id in movie_id_strings:
    url = "{0}{1}/credits?api_key={2}&language={3}".format(base_url, id, api_key, language)
    for i in range(MAX_ATTEMPTS):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()

                #Extend data with movie Id
                for cast_member in data["cast"]:
                    cast_member["movie_id"] = id
        
                cast_data.extend(data["cast"])

            print(f"Done for {id}")
            break  # break the loop if the request is successful
        except requests.exceptions.ConnectionError as e:
            if i == MAX_ATTEMPTS - 1:
                print(f"Maximum number of attempts reached. Skipping request {url}")
                break
            else:
                print(f"Attempt {i+1} failed. Retrying request {url}")
    
   


print("ended")
cast_schema = StructType([
    StructField("movie_id", IntegerType(), True),
    StructField("adult", BooleanType(), True),
    StructField("gender", IntegerType(), True),
    StructField("known_for_department", StringType(), True),
    StructField("name", StringType(), True),
    StructField("original_name", StringType(), True),
    StructField("popularity", FloatType(), True),
    StructField("profile_path", StringType(), True),
    StructField("cast_id", IntegerType(), True),
    StructField("character", StringType(), True),
    StructField("credit_id", StringType(), True),
    StructField("order", IntegerType(), True)
])



cast_df = spark.createDataFrame(cast_data, schema=cast_schema)

cast_df.write.mode("overwrite").csv(HDFS_NAMENODE + "/raw/cast.csv", header=True)

# stop the SparkSession
spark.stop()

