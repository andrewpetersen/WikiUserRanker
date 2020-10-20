# ---splitWikiXML.py---
# This script converts a large, multi-page XML file into many
# single-page XML files. 
# This script takes two input arguments: 'filename.xml' and 'outputFolder'
# Wiki XML files have a nested and repeating format. The first layer is 
# made of a <mediawiki> tag, which closes at the end of the file with 
# </mediawiki>. Within that layer is some information we don't need, 
# under the tag <siteinfo>. Next, there are hundreds of <page> tags, 
# each that end with </page>. Within each page, there are some number of 
# <revision> tags. This script scans each line, comparing it to either 
# '<page>' or '</page>' and opens or closes a new xml file, 
# respectively. All the data between the page tags is written to that 
# unique page xml file, along with the mediawiki and page tags. 
# The output filenames are the input filename + the sequence id number. 
# Note that this is different from the pageID number.
# The script also outputs the number of files and the execution time. 

import sys
import os
from time import perf_counter

tTotal = perf_counter()
fileToRead = sys.argv[1]
folderToWrite = sys.argv[2]

XMLStart = '<mediawiki>\n'
XMLEnd = '</mediawiki>\n'
lookingForPageEnd = False
pageCounter = 0
print('Splitting file ',fileToRead)
print('Creating directory for output: ',folderToWrite)
os.mkdir(folderToWrite)

with open(fileToRead) as infile:
    for line in infile:
        if lookingForPageEnd:
            outfile.write(line)
            if line=='  </page>\n':
                outfile.write(XMLEnd)
                outfile.close()
                lookingForPageEnd=False
                print('Closed page: ',pageCounter)
        elif line=='  <page>\n':
                pageCounter = pageCounter+1
                print('Found new page: ',pageCounter)
                outfile = open(folderToWrite+'/'+fileToRead+'_'+str(pageCounter).zfill(6), 'w')
                outfile.write(XMLStart)
                outfile.write(line)
                lookingForPageEnd=True
print("Page Count = ", pageCounter)
tTotalEnd = perf_counter()
TotalTime = tTotalEnd - tTotal
print("TotalTime: ",TotalTime)
