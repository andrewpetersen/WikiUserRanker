#
# ---XMLtoParquetSpark---
# Converts the varying-size XML files (each with one article's revision history)
# into parquet files for faster reading and processing later.
# The XML reader is slow and memory-hungry. This breaks that task into about 
# 50 smaller chunks (about 10 chunks per original ~30GB XML file)
# Took about 2 hours, 7 minutes to run on a Spark cluster with 4 worker nodes
# using AWS m5a.xlarge EC2 instances, reading from and writing to AWS S3.
#
# Andrew Petersen
# Oct. 18, 2020
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
keyRoot = 'wiki/p11000p11775/enwiki-latest-pages-meta-history1.xml-p11000p11775_000'
#keyRoot = 'wiki/p11776p12543/enwiki-latest-pages-meta-history1.xml-p11776p12543_000'
#keyRoot = 'wiki/p12544p13473/enwiki-latest-pages-meta-history1.xml-p12544p13473_000'
#keyRoot = 'wiki/p13474p13908/enwiki-latest-pages-meta-history1.xml-p13474p13908_000'
#keyRoot = 'wiki/p13909p14560/enwiki-latest-pages-meta-history1.xml-p13909p14560_000'
parquetKeyRoot = 'wiki-parquet/p110_'
#parquetKeyRoot = 'wiki-parquet/p117_'
#parquetKeyRoot = 'wiki-parquet/p125_'
#parquetKeyRoot = 'wiki-parquet/p134_'
#parquetKeyRoot = 'wiki-parquet/p139_'


if __name__ == "__main__":
    tTotal = perf_counter()

    # Setup Spark/SQL Contexts and Sessions
    tSetup = perf_counter()
    spark = SparkSession.builder\
                        .appName("XMLtoParquet")\
                        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")   
    spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com') #setup reading from AWS S3
    tSetupEnd = perf_counter()

    # Loading File Prep
    tLoadFile = perf_counter()

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
            StructField('text',StringType(),True)
        ])),True),
        StructField('title', StringType(),True)
    ])

    

    for i in range(10): #breaks the group of files into about 10 even chunks
        # Load files from S3
        key = keyRoot + '*' + str(i)
        print('Open Key: ',key)
        s3File = f's3a://{bucket}/{key}'
        df = spark.read.format("com.databricks.spark.xml") \
            .option("rowTag","page").load(s3File,schema=schema)
        tLoadFileEnd = perf_counter()

        #Write parquet files
        tWriteFile = perf_counter()
        parquetKey = parquetKeyRoot + str(i)
        print('ParquetKey: ',parquetKey)
        s3ParquetFiles = f's3a://{bucket}/{parquetKey}'
        df.repartition("id").write.partitionBy("id").parquet(s3ParquetFiles)
        tWriteFileEnd = perf_counter()
    
    tTotalEnd = perf_counter()

    TotalTime = tTotalEnd - tTotal
    SetupTime = tSetupEnd - tSetup
    LoadFileTime = tLoadFileEnd - tLoadFile
    WriteFileTime = tWriteFileEnd - tWriteFile

    print("TotalTime: ",TotalTime)
    print("SetupTime: ",SetupTime)
    print("LoadFileTime: ",LoadFileTime)
    print("WriteFileTime: ",WriteFileTime)

    spark.stop()
