from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import os
from pyspark.sql.window import Window
from pyspark.sql.functions import col, min, max, desc, split, array_contains, count, avg, explode, regexp_replace, trim, expr, regexp_extract


def convert_columns(movies_df):

    # Convert gross_income to double
    movies_df = movies_df.withColumn("gross_income", regexp_replace("gross_income", ",", "").cast("double"))
    movies_df = movies_df.filter(col("id").startswith("tt"))

    movies_df = movies_df.withColumn("genre", split(trim(col("genre")), ","))
    movies_df = movies_df.withColumn("directors_id", split(trim(col("directors_id")), ","))
    movies_df = movies_df.withColumn("directors_name", split(trim(col("directors_name")), ","))

    movies_df = movies_df.withColumn("genre", expr("transform(genre, x -> regexp_replace(x, ' ', ''))"))
    movies_df = movies_df.withColumn("directors_id", expr("transform(directors_id, x -> regexp_replace(x, ' ', ''))"))
    movies_df = movies_df.withColumn("directors_name", expr("transform(directors_name, x -> regexp_replace(x, ' ', ''))"))

    return movies_df

def filter_movies(movies_df):

    genre_pattern = r"^\s*(\D)"
    movies_df = movies_df.filter(~(array_contains(col("genre"), "Adult") | 
                                    array_contains(col("genre"), " Adult") |
                                    array_contains(col("genre"), "Game-Show") |
                                    array_contains(col("genre"), "Animation")))
    movies_df = movies_df.filter(regexp_extract(col("genre")[0], genre_pattern, 1).isin(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
                      
    return movies_df


def top_hundred_movies(movies_df):
    # Calculate weighted rating based on vote_average and vote_count
    final_df = movies_df.withColumn("weighted_rating", 
                                (col("votes") / (col("votes") + 250) * col("rating")) + 
                                (250 / (col("votes") + 250) * 7.0))

    # Sort dataframe by weighted rating and take top 100 rows
    top_100_movies = final_df.orderBy(col("weighted_rating").desc()).limit(100)

    selected_columns = ["id", "name", "year", "genre", "rating", "votes", "weighted_rating", "directors_id","directors_name"]
    selected_df = top_100_movies.select([col(column_name) for column_name in selected_columns])

    write_df(selected_df,"top_100_movies")

def gross_income_by_genre(movies_df):
    genre_pattern = r"^\s*(\D)"
    movies_df = movies_df.filter(regexp_extract(col("genre")[0], genre_pattern, 1).isin(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
    selected_columns = ["id", "name", "year","genre" ,"gross_income"]
    selected_df = movies_df.select([col(column_name) for column_name in selected_columns])
    movies_df = selected_df.withColumn("genre", explode("genre"))

    # Calculate the average gross_income by genre
    avg_gross_income_by_genre = movies_df.groupBy("genre").agg(avg("gross_income").alias("avg_gross_income"))
    sorted_df = avg_gross_income_by_genre.orderBy(desc("avg_gross_income"))

    write_df(sorted_df,"income_by_genre")

def directors(movies_df) :

    df_exp = movies_df.selectExpr("*", "arrays_zip(directors_id, directors_name) as dir_id_name")
    df_exploded = df_exp.selectExpr("*", "explode(dir_id_name) as dir_info")
    df_result = df_exploded.selectExpr("id", "name", "year", "genre", "gross_income" , 
                                    "dir_info.directors_id as director_id", 
                                    "dir_info.directors_name as director_name")

    df = df_result.where(col("director_id").startswith("nm"))
   
    final_df = df.groupBy("director_id","director_name")\
                                    .agg(avg("gross_income").alias("avg_gross_income")) \
                                    .orderBy(desc("avg_gross_income")) \
                                    .select("director_name", "avg_gross_income") \
                                    .limit(100)

    write_df(final_df,"directors")

# Function counts the number of movies in each certificate category. 
def num_movies_by_certificate_category(movies_dfs) :

    wanted_certs = ["TV-14", "TV-G", "TV-PG", "Not Rated", "TV-MA", "TV-Y", "TV-Y7", "R", "PG", "Approved", "PG-13", "TV-Y7-FV", "Unrated", "T", "Passed", "E10+", "E", "G", "M", "K-A", "GP", "12", "X", "PG-12", "TV-13", "MA-13", "AO", "NC-17", "R-12", "MA-17", "M/PG", "R-15", "Open", "EC", "GA", "Banned", "R-18", "CE"]
    filtered_df = movies_dfs.filter(col("certificate").isin(wanted_certs))
    windowSpec = Window.partitionBy("certificate")

    # Count the number of times ceftificate appears in the dataframe
    df = filtered_df.withColumn("count", count("certificate").over(windowSpec))

    # Display the results
    df= df.select("certificate", "count").distinct()
    df = df.orderBy(desc("count"))
   
    write_df(df,"movies_in_certificate_category")

def rating_by_genre(movie_df):
   
 
  write_df(movie_df, "Genre_popularity")


def genre_min_and_max_grossing_movie(movies_df):
    
    genre_pattern = r"^\s*(\D)"
    movies_df = movies_df.filter(regexp_extract(col("genre")[0], genre_pattern, 1).isin(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
    selected_columns = ["id", "name", "year","genre" ,"gross_income"]
    selected_df = movies_df.select([col(column_name) for column_name in selected_columns])
    df = selected_df.withColumn("genre", explode("genre"))


    windowSpec = Window.partitionBy("genre","gross_income")    
    # print(type(df))
    # df.printSchema()
    # df.show(10)

    #Compute the minimum and maximum rating for each movie by director
    df_min_max = df.select("genre") \
                .withColumn("worst_grossing", min("gross_income").over(windowSpec)) \
                .withColumn("best_grossing", max("gross_income").over(windowSpec)) \
                .orderBy(desc("best_grossing")) \
                .limit(200) 
    
    write_df(df_min_max,"best_and_worst")


def the_movie_duration(movie_df):
## percent movies lasting one hour
## percent movies lasting 2 hours
## percent movies lasting 3 hours
## percent movies lasting >3 hours   
    write_df(movie_df, "Genre_popularity")

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
    
    movie_schema = StructType([
    StructField("id", StringType(), True, metadata={"pattern": "^tt.*"}),
    StructField("name", StringType(), True),
    StructField("year", StringType(), True),
    StructField("rating", DoubleType(), True),
    StructField("certificate", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("votes", IntegerType(), True),
    StructField("gross_income", StringType(), True),
    StructField("directors_id", StringType(), True),
    StructField("directors_name", StringType(), True),
    StructField("stars_id", StringType(), True),
    StructField("stars_name", StringType(), True),
    StructField("description", StringType(), True)
    ])
    
    movies_df = spark.read.csv(HDFS_NAMENODE + "/raw/movies.csv", header=True,schema=movie_schema)
    movies_df.cache()
    
    df = convert_columns(movies_df)
    # directors(df)
    # num_movies_by_certificate_category(df)

    # filtered_movies_df = filter_movies(df)

    # top_hundred_movies(filtered_movies_df)

    #gross_income_by_genre(df)

    genre_min_and_max_grossing_movie(df)

    movies_df.unpersist()


  