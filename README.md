# WikiRanks
Identifying bad actors in the Wikipedia community. [Presentation Slides](https://docs.google.com/presentation/d/1m59FcSo41q1idPtoMPlCjy3I6IXYj86cdhd5BH_uTdY)

To make an open and collaborative project like Wikipedia work, the community needs to identify bad actors to prevent them from destructive behavior. 

Using Wikipedia’s historical dataset of the complete edit history for every article, the longevity of every edit can be calculated. 
For each user, calculating the average persistence of all their edits would give a general metric of the quality of their edits. 
This can identify malicious editors whose edits are consistently being reverted or changed. 

## Wikipedia Dataset
Wikipedia keeps the complete revision history of every article and makes it accessible to the public at https://dumps.wikimedia.org/enwiki/latest/ (Consider downloading from a mirror instead).
Each data dump, at least for the English Wikipedia, consists of several hundred compressed XML files. For the complete edit history, look for files with 'pages-meta-history' in their name. 

## Pipeline
<img src="assets/pipeline.PNG">

With an Amazon EC2 instance, files were downloaded, decompressed, split by article, then moved to Amazon S3. Using Apache PySpark, a cluster of Amazon EC2 instances were first used to convert the XML files to Parquet files. Once complete, the cluster loaded and merged all the data from the parquet files to score the individual editors based on the lifetime of their revisions. The ranked list of users was stored in a Postgresql database, which is then accessible to a Dash web app to display the calculated scores and ranking of the users.  

## Challenges
##### Decentralized Documentation
Wikipedia, while a wonderful repository for knowledge, still sometimes feels like a big group project. The documentation for how Wikipedia's data dumps were organized was difficult to find and spread out among many different places. Besides looking directly at the downloaded XML files, the next best source of information about the Wikipedia's data files was from Wikipedia User [Emijrp](https://en.wikipedia.org/wiki/User:Emijrp/Wikipedia_Archive) on his personal user page. 

##### Ingestion of Large XML Files
The XML file format is not the format of choice for most purposes. Unfortunately, I was unable to find a python library that has been designed to read and parse very large XML files. (However, it should be noted that the library sometimes worked with very large files - but only with relatively equal article lengths.) To solve this issue quickly, I wrote a simple python script to cut the large XML files into hundreds of smaller ones, keeping all information from a single article together. This allowed the XML reader library to be able to ingest the data, albeit a little slowly, then save the data as Parquet files for faster use later. Using predefined table schema here was also important.

##### Optimizing the Scoring Computation
While the revision lifetime scoring algorithm is not complex, running it quickly is important. I benchmarked a few different methods to match revisions with the revision that replaced it. Of note, a dataframe self-join was compared to a lag window-function after grouping by article and sorting by timestamp. (86 seconds and 55 seconds, respectively, for a 36% reduction in execution time for this step.)  

## About and License

WikiRanks was created and completed during my time as part of the 2020C Los Angeles, Data Engineering cohort at [Insight](https://insightfellows.com/).

GNU General Public License, version 3 (GPLv3), see full text of license in LICENSE.txt.

Andrew Petersen

Last Updated:
Oct. 19, 2020
