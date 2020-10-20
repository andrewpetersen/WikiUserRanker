# WikiRanks
Identifying bad actors in the Wikipedia community. [Presentation Slides](https://docs.google.com/presentation/d/1m59FcSo41q1idPtoMPlCjy3I6IXYj86cdhd5BH_uTdY)

To make an open and collaborative project like Wikipedia work, the community needs to identify bad actors to prevent them from destructive behavior. 

Using Wikipedia’s historical dataset of the complete edit history for every article, the longevity of every edit can be calculated. 
For each user, calculating the average persistence of all their edits would give a general metric of the quality of their edits. 
This can identify malicious editors whose edits are consistently being reverted or changed. 

## Wikipedia Dataset
Wikipedia keeps the complete revision history of every article and makes it [accessible to the public.](https://dumps.wikimedia.org/enwiki/latest/)
Each data dump, at least for the English Wikipedia, consists of several hundred compressed XML files. For the complete edit history, look for files with 'pages-meta-history' in their name. 

## Pipeline
<img src="assets/pipeline.PNG">

With an Amazon EC2 instance, files were downloaded, decompressed, split by article, then moved to Amazon S3. Using Apache PySpark, a cluster of Amazon EC2 instances were first used to convert the XML files to Parquet files. Once complete, the cluster loaded and merged all the data from the parquet files to score the individual editors based on the lifetime of their revisions. The ranked list of users was stored in a Postgresql database, which is then accessible to a Dash web app to display the calculated scores and ranking of the users.  

## Setup
The full pipeline was implemented with AWS resources. For downloading the compressed files, decompressing the files, and splitting the files with **splitXMLs.py**, an m5a.large EC2 instance was used. To setup the Spark cluster with 5 EC2 instances (1 m5a.large controller with 4 m5a.xlarge workers). The PostgreSQL database was hosted on another m5a.large instance, and the Dash on Flask web app was hosted with a t2.micro instance. 
Some specific setup instructions are available here: [Spark Cluster](https://blog.insightdatascience.com/create-a-cluster-of-instances-on-aws-899a9dc5e4d0), [S3](https://blog.insightdatascience.com/create-a-cluster-of-instances-on-aws-899a9dc5e4d0), [PostgreSQL](https://blog.insightdatascience.com/simply-install-postgresql-58c1e4ebf252), [Flask](https://github.com/InsightDataScience/flask-sample-app)/[Dash](https://www.youtube.com/watch?v=hSPmj7mK6ng)

## Usage
Download compressed files onto a Linux machine (eg, an EC2 instance). Go to https://dumps.wikimedia.org/enwiki/latest/ to choose exactly which types of files you want to download. Consider downloading an older dataset from a mirror instead.
```sh
wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-meta-history1.xml-p13474p13908.7z
```

Decompress the files.
```sh
sudo apt install p7zip-full
7za e filename.7z 
```

Split the large XML files into smaller XML files.
```sh
python3 splitWikiXML.py filename.xml outFolderName
```

Move the file to S3. Setup your S3 bucket and credentials with AWS CLI to use this command. 
```sh
aws s3 cp outFolderName s3://bucketName/outFolderName --recursive
```

With the PySpark cluster, convert the files to Parquet files. 
```sh
spark-submit --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7,org.postgresql:postgresql:42.2.16.jre7 --conf spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true --conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true --jars spark-xml_2.11-0.10.0.jar --master spark://10.0.0.8:7077 XMLtoParquetSpark.py
```

Then, compute the contributor rankings.
```sh
spark-submit --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7,org.postgresql:postgresql:42.2.16.jre7 --conf spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true --conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true --jars spark-xml_2.11-0.10.0.jar --master spark://10.0.0.8:7077 WikiUserRankerSpark.py

```

## Challenges
##### Decentralized Documentation
Wikipedia, while a wonderful repository for knowledge, still sometimes feels like a big group project. The documentation for how Wikipedia's data dumps were organized was difficult to find and spread out among many different places. Besides looking directly at the downloaded XML files, the next best source of information about the Wikipedia's data files was from Wikipedia User [Emijrp](https://en.wikipedia.org/wiki/User:Emijrp/Wikipedia_Archive) on his personal user page. 

##### Ingestion of Large XML Files
The XML file format is not the format of choice for most purposes. Unfortunately, I was unable to find a python library that has been designed to read and parse very large XML files. (However, it should be noted that the library sometimes worked with very large files - but only with relatively equal article lengths.) To solve this issue quickly, I wrote a simple python script to cut the large (~30GB) XML files into hundreds of smaller ones, keeping all information from a single article together. This allowed the XML reader library to be able to ingest the data, albeit a little slowly, then save the data as Parquet files for faster use later. Using predefined table schema here was also important.

##### Optimizing the Scoring Computation
While the revision lifetime scoring algorithm is not complex, running it quickly is important. I benchmarked a few different methods to match revisions with the revision that replaced it. Of note, a dataframe self-join was compared to a lag window-function after grouping by article and sorting by timestamp. (86 seconds and 55 seconds, respectively, for a 36% reduction in execution time for this step.)  

## About and License

WikiRanks was created and completed during my time as part of the 2020C Los Angeles, Data Engineering cohort at [Insight](https://insightfellows.com/).

GNU General Public License, version 3 (GPLv3), see full text of license in LICENSE.txt.

Andrew Petersen

Last Updated:
Oct. 19, 2020
