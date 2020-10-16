#
# ---WikiRanks---
# This code reads hundreds of XML files of article revisions from Amazon S3, 
# converts the XMLs to a dataframe with python-xml, cleans the data a bit, 
# then matches each revision to time it was replaced by the next revision. 
# With this, a crude 'score' for every contributor based on the lifetime of 
# their revisions is calculated. The final table of contributors and scores
# is written to Postgres. 
# Andrew Petersen
# Oct. 15, 2020
# 
#

from __future__ import print_function

import sys
import os
from operator import add

from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType
from decimal import Decimal

from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import functions as func

from pyspark.sql import Window
from pyspark.sql import SQLContext

from time import perf_counter,time

#parameters to set
region = 'us-west-1'
bucket = 'petersen-insight-s3'
dateOfDataDumpInEpochSeconds = 1602528423
key = 'supernova20200916.xml'
#dateOfSeconds = 1585766886
#key = 'trwiki-latest-pages-meta-history.xml'
#key = 'wiki/p11776p12543/enwiki-latest-pages-meta-history1.xml-p11776p12543_00018*'
key = 'wiki/p11776p12543/enwiki-latest-pages-meta-history1.xml-p11776p12543_000*'
#key = 'wiki/p11776p12543/enwiki-latest-pages-meta-history1.xml-p11776p12543_000249'

if __name__ == "__main__":
    tTotal = perf_counter()

    # Setup Spark/SQL Contexts and Sessions
    tSetup = perf_counter()
    spark = SparkSession.builder\
                        .appName("WikiRanks")\
                        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")   #Hide unnecessary status information from standard output
    spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com') #setup reading from AWS S3
  #  sqlContext = SQLContext(sc)
    tSetupEnd = perf_counter()

    # Loading File Prep
    tLoadFile = perf_counter()
    s3file = f's3a://{bucket}/{key}'

    schema = StructType([ # read per page
        StructField('ns',IntegerType(),True),
        StructField('id',IntegerType(),True),
        StructField('revision', ArrayType(StructType([
            StructField('id',IntegerType(),True),
            StructField('parentid',IntegerType(),True),
            StructField('timestamp',TimestampType(),True),
            StructField('contributor',StructType([
                StructField('ip',StringType(),True),
                StructField('username',StringType(),True)
            ]),True),
        ])),True),
        StructField('title', StringType(),True)
    ])

    # Load files from S3
    df = spark.read.format("com.databricks.spark.xml") \
        .option("rowTag","page").load(s3file,schema=schema)\
        .filter((func.col("ns")==0))\
        .repartition(500)
    df.show()
    tLoadFileEnd = perf_counter()

    df.printSchema()
    print("df.dtypes: ",df.dtypes)

    #Count the number of articles, by counting rows
    numPgRows = df.count()
    print("Number of Pages (Articles) = ",numPgRows)
    
    # Table currently: 1 row = 1 article or page
    # Expand each row into many rows so that 1 row = 1 revision. Rename columns to prevent naming conflicts
    tExplodeData = perf_counter()
    df = df.select(col("title").alias("pageTitle"), col("id").alias("pageID"), col("ns").alias("pageNS"), func.explode("revision").alias("rev"))\
        .select("pageTitle", "pageID", "pageNS", "rev.*")\
        .select("pageTitle", "pageID","pageNS", col("id").alias("revisionID"), col("parentid").alias("revisionParentID"), col("timestamp"), col("contributor")) #, col("text"))
    tExplodeDataEnd = perf_counter()
    numRev = df.count()
    df.show()
    print("Exploded Pages to Revisions\nNumber of Revisions: ",numRev)
#    df.show(numRev,truncate=False)

    # Clean user information, adding 2 columns: cleanContributor and isRegistered
    tCleanUsers = perf_counter()
    df = df.withColumn("isRegistered", when(func.length(col("contributor.username"))!=0,True).otherwise(False))
    df = df.withColumn("cleanContributor",when(col("isRegistered"),col("contributor").username).otherwise(col("contributor").ip))
    tCleanUsersEnd = perf_counter()
    df.show()
    print("Cleaned user information")

    # Convert all timestamps to Unix Epoch Seconds, and remove old timestamp
    tTimestamp = perf_counter()
    df = df.withColumn("EpochTimestampStart",func.to_timestamp("timestamp").cast("long"))
    dfScore = df.select("pageTitle","pageID","cleanContributor","revisionID","revisionParentID","EpochTimestampStart","isRegistered")
    tTimestampEnd = perf_counter()
    dfScore.show()

    # Write to Postgresql working database
   # tWriteDB = perf_counter()
    #url = 'jdbc:postgresql://10.0.0.11:5442/cluster_active'
   # postgresUser = os.environ['POSTGRES_USER']
   # postgresPass = os.environ['POSTGRES_PASS']
   # properties = {"user":postgresUser,"password":postgresPass,"driver":"org.postgresql.Driver"}
   # allArticles.write.jdbc(url=url, table="AllArticles",mode="overwrite",properties=properties)
   # tWriteDBEnd = perf_counter()

    # group by article, sort by timestamp, then shift and subtract
#    df.unpersist()
#    approxCount = dfScore.rdd.countApprox(timeout=1000)
#    print("\n\napproxCount: ",approxCount,"\n\n")
    
    # Self-join to match every revision with the revision timestamp that replaced it
    tMatchRevs = perf_counter()
    df3 = dfScore.select(col("pageID").alias("pageID2"),col("revisionParentID").alias("revisionID"),col("EpochTimestampStart").alias("EpochTimestampEnd"))
    df3.show()
    dfScore = dfScore.join(df3, (dfScore.pageID == df3.pageID2) & (dfScore.revisionID == df3.revisionID))
    print("Joined!")
    tMatchRevsEnd = perf_counter()
    dfScore.show()

    #Instead of a join, could use a window function with a lag on data sorted by timestamp after grouping by pageID
#    window = Window.partitionBy("pageID").orderBy("EpochTimestampStart")
#    dfScore = dfScore.withColumn("EpochTimestampEnd", func.lag(dfScore.EpochTimestampStart,-1).over(window))
#    dfScore.show()

    # Subtract startTime from endTime to get lifeTime of each revision
    tScoring = perf_counter()
    dfScore = dfScore.withColumn("LiveSeconds", func.when(func.isnull(dfScore.EpochTimestampEnd - dfScore.EpochTimestampStart),dateOfDataDumpInEpochSeconds-dfScore.EpochTimestampStart).otherwise(dfScore.EpochTimestampEnd - dfScore.EpochTimestampStart))
    dfScore.show()
    print("Table with Lifetime Calcs ^^^")
    
    # Score each contributor based on the average and total life of their revisions
    contributors = Window.partitionBy("cleanContributor")
    outDf = dfScore.withColumn("Score-Sum", func.sum("LiveSeconds").over(contributors)) \
              .withColumn("Score-Avg", func.avg("LiveSeconds").over(contributors)) \
              .withColumn("Score-Count", func.count("LiveSeconds").over(contributors)) \
              .select("cleanContributor","Score-Sum","Score-Avg","Score-Count","isRegistered").distinct().orderBy("cleanContributor","Score-Sum","Score-Avg","Score-Count",ascending=False)
    numContributors = outDf.count()
    tScoringEnd = perf_counter()
#    outDf.show(numContributors,truncate=False)
    outDf.show()
    print("Total number contributors: ",numContributors)

    # Write output to Postgresql database
    dbTableName ="ContributorScores6"
    tWriteDB = perf_counter()
    url = 'jdbc:postgresql://10.0.0.11:5442/cluster_output'
    postgresUser = os.environ['POSTGRES_USER']
    postgresPass = os.environ['POSTGRES_PASS']
    properties = {"user":postgresUser,"password":postgresPass,"driver":"org.postgresql.Driver"}
    outDf.write.jdbc(url=url, table=dbTableName,mode="overwrite",properties=properties)
    tWriteDBEnd = perf_counter()

    # Read from Postgresql database to confirm prior write
    tReadDB = perf_counter()
    readSqlDf = spark.read.jdbc(url=url,table=dbTableName,properties=properties)
    readSqlDf.show()
    tReadDBEnd = perf_counter()

    # Deal with timings
    tTotalEnd = perf_counter()

    TotalTime = tTotalEnd - tTotal
    SetupTime = tSetupEnd - tSetup
    LoadFileTime = tLoadFileEnd - tLoadFile
    ExplodeDataTime = tExplodeDataEnd - tExplodeData
    CleanUsersTime = tCleanUsersEnd - tCleanUsers
    TimestampTime = tTimestampEnd - tTimestamp
    MatchRevsTime = tMatchRevsEnd - tMatchRevs
    ScoringTime = tScoringEnd - tScoring
    WriteDBTime = tWriteDBEnd - tWriteDB
    ReadDBTime = tReadDBEnd - tReadDB

    print("Input File: ",key)
    print("Output Table: ",dbTableName)
    print("TotalTime: ",TotalTime)
    print("SetupTime: ",SetupTime)
    print("LoadFileTime: ",LoadFileTime)
    print("ExplodeDataTime: ",ExplodeDataTime)
    print("CleanUsersTime: ",CleanUsersTime)
    print("TimestampTime: ",TimestampTime)
    print("MatchRevsTime: ",MatchRevsTime)
    print("ScoringTime: ",ScoringTime)
    print("WriteDBTime: ",WriteDBTime)
    print("ReadDBTime: ",ReadDBTime)

    spark.stop()

