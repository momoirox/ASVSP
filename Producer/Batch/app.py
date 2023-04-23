import csv
from pyspark.sql.session import SparkSession
import os
import time
import sys 

if __name__ == '__main__':

    HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

    spark = SparkSession\
        .builder\
        .appName("HDFSData")\
        .getOrCreate()

    df = spark.read.csv("../Data/movies.csv", header=True)
    df.write.csv(HDFS_NAMENODE + "/raw/movies.csv",header=True, mode="overwrite")

  



