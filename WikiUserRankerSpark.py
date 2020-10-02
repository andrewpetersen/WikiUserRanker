#
# ---WikiUserRanker---
# This code reads an XML file of article revisions from Amazon S3, 
# converts the XML to a dataframe with python-xml, then calculates 
# a crude 'score' for every contributor based on the lifetime of 
# every one of their revisions. The final table of contributors and 
# scores is written to Postgres. 
# Andrew Petersen
# Oct. 2, 2020
# 
#

from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from decimal import Decimal

from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import functions as func

from pyspark.sql import Window
from pyspark.sql import SQLContext

#parameters to set
region = 'us-west-1'
bucket = 'petersen-insight-s3'
key = 'supernova20200916.xml'
appName = "Petersen_XML_S3_Postgres_Testing"
#master = "local" #add .master(master) after appName()?? #I think this is dealt with in the cmd line spark-submit call

if __name__ == "__main__":
    print("Length of sys.argv: ", len(sys.argv)) #Print number of input arguments
    print("sys.argv: ",sys.argv) #Print input arguments
    sc = SparkContext()
    sc.setLogLevel("ERROR") #Hide unnecessary status information from standard output
    sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)
    s3file = f's3a://{bucket}/{key}'
    
# Should specify schema for faster ingestion:
#    schema = StructType([
#        StructField('id', IntegerType(),False),
#        StructField('parentid', IntegerType(),False),
#        StructField('contributer', StructType(),False),
#        StructField('timestamp', StringType(),False)
#    ])

    df = spark.read.format("com.databricks.spark.xml") \
        .option("rowTag","revision").load(s3file)
    #    .option("rowTag","revision").load(s3file, schema=schema)  # switch to include predefined schema
    df.show()
    
    # Convert Contributor column from type Struct to type String
    df = df.withColumn("contributor", func.col("contributor").cast("String"))
    print("df.dtypes: ",df.dtypes)

    # using timestamp column, convert from string to timestamp, convert to epoch seconds, shift and subtract.
    df = df.withColumn("EpochTimestampStart",func.to_timestamp("timestamp").cast("long"))
    window = Window.partitionBy().orderBy("EpochTimestampStart")
    df = df.withColumn("EpochTimestampEnd", func.lag(df.EpochTimestampStart,-1).over(window))
    df = df.withColumn("LiveSeconds", func.when(func.isnull(df.EpochTimestampEnd - df.EpochTimestampStart), 0).otherwise(df.EpochTimestampEnd - df.EpochTimestampStart))
    df.show()

    # Score each contributor based on the average and total life of their revisions
    contributors = Window.partitionBy("contributor")
    scoreDf = df.withColumn("Score-Sum", func.sum("LiveSeconds").over(contributors)) \
              .withColumn("Score-Avg", func.avg("LiveSeconds").over(contributors)) \
              .withColumn("Score-Count", func.count("LiveSeconds").over(contributors)) \
              .select("contributor","Score-Sum","Score-Avg","Score-Count").distinct()
    totalRows = scoreDf.count()
    scoreDf.show(totalRows,truncate=False)
    
    # Write output to Postgresql database
    url = 'jdbc:postgresql://10.0.0.11:5442/cluster_output'
    properties = {"user":"*redacted*","password":"*redacted*","driver":"org.postgresql.Driver"}
    scoreDf.write.jdbc(url=url, table="ContributorScores",properties=properties)
    
    # Read from Postgresql database to confirm prior write
    readSqlDf = spark.read.jdbc(url=url,table="ContributorScores",mode="overwrite",properties=properties) 
    readSqlDf.show(totalRows);

    spark.stop()
