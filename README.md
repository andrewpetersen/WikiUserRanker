# WikiRanks
Identifying bad actors in the Wikipedia community.

To make an open and collaborative project like Wikipedia work, the community needs to identify bad actors to prevent them from destructive behavior.

Using Wikipedia’s historical dataset of the complete edit history for every article, the longevity of every edit can be calculated. 
For each user, calculating the average persistence of all their edits would give a general metric of the quality of their edits. 
This can identify malicious editors whose edits are consistently being reverted or changed.

The overall pipeline:
Wikipedia keeps the complete revision history of every article and makes it accessible to the public (https://dumps.wikimedia.org/). Consider downloading from a mirror instead. 
Data was uncompressed and moved to Amazon S3. Using PySpark, a cluster of Amazon EC2 instances running Ubuntu 18 were used to load the data and score the individual editors based on the lifetime and viewership of their revisions. The ranked list of users was stored in a Postgresql database, which was used as the data source for a web app (unimplemented) to display the calculated scores and ranking of users.  

This project was created and completed during my time as part of the 2020C Los Angeles, Data Engineering cohort at Insight (https://insightfellows.com/).

Last Updated:
Andrew Petersen
Oct. 11, 2020
