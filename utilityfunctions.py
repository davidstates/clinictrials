__author__ = 'David States'

import xml.etree.ElementTree as ET
import urllib.request
import psycopg2
import psycopg2.extensions

#
# define a utility function to excute and SQL statement
#
class MyCursor(psycopg2.extensions.cursor):
    def executesql(self, statement):
        try:
            self.execute(statement)
        except:
            print("Unable to execute SQL")
            print(statement)
            exit()

    def executefetchone(self, statement):
        # noinspection PyBroadException
        try:
            self.execute(statement)
            qret = self.fetchone()
            return(qret)
        except:
            print("Unable to execute SQL and fetch")
            print(statement)
            exit()

# define a utility function to retrieve the text of an XML node if it exists
# Also strip out single quotes so they do not cause problems in SQL or other quotes
def getXmlText(root, xpath):
    nd = root.find(xpath)
    if nd is None:
        val = ""
    else:
        tlist = nd.itertext()
        val = ''.join(tlist)
        if val is None:
            val = ""
        val = val.replace("'", "''")

    return(val)

# define a function to fetch the XML file from clinicaltrials.gov
def fetchClinicalTrials(nct_id):
    url = 'https://clinicaltrials.gov/show/'+nct_id+ '?resultsxml=true'
    print('Fetching '+url)
    with urllib.request.urlopen(url) as response:
        xd = response.read()
        root = ET.fromstring(xd)
    return(root)
