
fileRoot = 'enwiki-latest-pages-meta-history1.xml-'
#fileSuffix = 'p11000p11775'
#fileSuffix = 'p11776p12543'
#fileSuffix = 'p12544p13473'
#fileSuffix = 'p13474p13908'
fileSuffix = 'p13909p14560'
fileToRead = fileRoot+fileSuffix

XMLStart = '<mediawiki>\n'
XMLEnd = '</mediawiki>\n'
lookingForPageEnd = False
pageCounter = 0
print('Splitting file ',fileToRead)

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
                outfile = open(fileSuffix+'/'+fileToRead+'_'+str(pageCounter).zfill(6), 'w')
                outfile.write(XMLStart)
                outfile.write(line)
                lookingForPageEnd=True
print("Page Count = ", pageCounter)
