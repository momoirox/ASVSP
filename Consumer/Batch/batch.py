from pyspark.sql.session import SparkSession
import os
from pyspark.sql.functions import col
from pyspark.sql.functions import avg

def rating_by_genre(movie_df):
   
    weighted_movie_df = movie_df.withColumn("weighted_rating", 
                                (col("vote_count") / (col("vote_count") + 250) * col("vote_average")) + 
                                (250 / (col("vote_count") + 250) * 7.0))
    

    avg_weighted_rating_df = weighted_movie_df.groupBy("genre_name") \
        .agg(avg("weighted_rating").alias("avg_weighted_rating"))

    
    genres_final = avg_weighted_rating_df.orderBy(col("avg_weighted_rating").desc())
    write_df(genres_final, "Genre_popularity")
   
def top_ten_movies(movie_df):
    # Calculate weighted rating based on vote_average and vote_count
    final_df = movie_df.withColumn("weighted_rating", 
                                (col("vote_count") / (col("vote_count") + 250) * col("vote_average")) + 
                                (250 / (col("vote_count") + 250) * 7.0))

    # Sort dataframe by weighted rating and take top 100 rows
    top_100_movies = final_df.orderBy(col("weighted_rating").desc()).limit(100)

    write_df(top_100_movies, "Top_100_Movies")
   

def write_df(dataframe,tablename):
    PSQL_SERVERNAME= "postgres"
    PSQL_PORTNUMBER = 5432
    PSQL_DBNAME = "postgres"
    PSQL_USERNAME = "postgres"
    PSQL_PASSWORD = "postgres"
    URL = f"jdbc:postgresql://{PSQL_SERVERNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"

    dataframe.write.format("jdbc").options(
        url=URL,
        driver="org.postgresql.Driver",
        user=PSQL_USERNAME,
        password=PSQL_PASSWORD,
        dbtable=tablename
    ).mode("overwrite").save()


if __name__ == '__main__':

    HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

    spark = SparkSession\
        .builder\
        .appName("BatchProcessing")\
        .getOrCreate()
    
    movie_genres_df = spark.read.csv(HDFS_NAMENODE + "/raw/movies_and_genres.csv", header=True)
    ##ratings_df = spark.read.csv(HDFS_NAMENODE + "/raw/movie_ratings.csv", header=True)

    #top_ten_movies(movie_genres_df)
    rating_by_genre(movie_genres_df)