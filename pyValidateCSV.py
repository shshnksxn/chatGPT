import commands
import sys
import re
import time
from optparse import OptionParser
import csv
import math
from sets import Set
from decimal import *

mStr = ''
dStr = ''
startTime = ''
endTime = ''
dbName = ''
dbPort = ''
ip = ''
cubeName = ''
mListStr = ''
mMap = {}


class DictDiffer(object):
    """
    Calculate the difference between two dictionaries as:
    (1) items added
    (2) items removed
    (3) keys same in both but changed values
    (4) keys same in both and unchanged values
    """
    def __init__(self, current_dict, past_dict):
        self.current_dict, self.past_dict = current_dict, past_dict
        self.set_current, self.set_past = set(current_dict.keys()), set(past_dict.keys())
        self.intersect = self.set_current.intersection(self.set_past)
    def added(self):
        return self.set_current - self.intersect 
    def removed(self):
        return self.set_past - self.intersect 
    def changed(self):
	changedSet = Set()
        for o in self.intersect:
		if (len(self.past_dict[o]) > 1) and (len(self.current_dict[o]) > 1) and (len(self.past_dict[o]) == len(self.current_dict[o])):
			for i in range(len(self.past_dict[o])):
				if(abs(self.past_dict[o][i]-self.current_dict[o][i]) > 0.005):
                        		changedSet.add(o)
					break
		else:
			if (abs(self.past_dict[o][0]-self.current_dict[o][0]) > 0.005):
				changedSet.add(o)
	return changedSet
    def unchanged(self):
        return set(o for o in self.intersect if self.past_dict[o] == self.current_dict[o])


def getConfigStats(genConf="csvConfig.conf"):

        ##########################################################################################
        #          This function fethes the configuration data from the config.conf file         #
        ##########################################################################################
	global startTime
	global endTime
	global dbName
	global dbPort
	global ip
	global cubeName
	global mStr
	global dStr
	global mListStr
	global mMap
        try:
                f = open(genConf, 'r')
                perfFile = f.readlines()
                for line in perfFile:
			if 'Start_Time' in line:
                                m = re.search('.+?=\s+(.*)',line)
                                startTime = m.group(1).rstrip('\r\n')
                        if 'End_Time' in line:
                                m = re.search('.+?=\s+(.*)',line)
                                endTime = m.group(1).rstrip('\r\n')
                        if 'DB_Name' in line:
                                m = re.search('.+?=\s+(.*)',line)
                                dbName = m.group(1).rstrip('\r\n')
                        if 'DB_Port' in line:
                                m = re.search('.+?=\s+(.*)',line)
                                dbPort = m.group(1).rstrip('\r\n')
                        if 'iNSTA_IP' in line:
                                m = re.search('.+?=\s+(.*)',line)
                                ip = m.group(1).rstrip('\r\n')
                        if 'CUBE_Name' in line:
                                m = re.search('.+?=\s+(.*)',line)
                                cubeName = m.group(1).rstrip('\r\n')
                        if 'Measure_List' in line:
                                m = re.search('.+?=\s+(.*)',line)
                                mList = m.group(1).rstrip('\r\n').split(',')
                                for i in mList:
                                        mListStr = mListStr + ":" + i + ".None"
                                mListStr = mListStr[1:]
                        if 'CSV_HDR_DIM' in line:
                                d = re.search('.+?=\s+(.*)',line)
                                dList = d.group(1).rstrip('\r\n').split(',')
                                for i in dList:
                                        dStr = dStr + "^" + i
                                dStr = dStr[1:]
			if 'CSV_HDR_MSR' in line:
				m = re.search('.+?=\s+(.*)',line)
                                mList = m.group(1).rstrip('\r\n').split(',')
                                for i in mList:
                                        mStr = mStr + "^" + i
                                mStr = mStr[1:]
			if 'MEASURE_MAP' in line:
				m = re.search('.+?=\s+(.*)',line)
				mPair = m.group(1).rstrip('\r\n').split(',')
				for mp in mPair:
					(mKey, mVal) = mp.split(':')
					if mMap.has_key(mKey):
						print "Duplicate entry, updating the map with the latest value"
					mMap[mKey] = ''
					mMap[mKey] = mVal
				print "mMap is %s" %(mMap)

        except IOError:
                print "Can't read file. Please check if the config.conf file is present in the current directory"
                sys.exit(0)

def getTupleConfigStats(tupleConfFile):

        f = open(tupleConfFile, 'r')
        conf = f.readlines()
        fStr = ''


        for l in conf:
                val = l.rstrip('\n').split('=')
                #print val

                if val[1] is not '0':
                        fStr = fStr+"*"+val[1]
                elif (val[1] is '0'):
                        fStr = fStr+"*-1"
                else:
                        print "Error in parameters"
                        exit(-1)
        return fStr[1:]

def getFilterConfigStats(filterConfFile):
	f = open(filterConfFile, 'r')
        conf = f.readlines()
        fKey = ''


        for l in conf:
                val = l.rstrip('\n').split('=')
                #print val

                if val[1] is not '0':
                        fKey = fKey+"*"+val[1]
                elif (val[1] is '0'):
                        fKey = fKey+"*0"
                else:
                        print "Error in parameters"
                        exit(-1)
        return fKey[1:]


def genStringIdMap(database):
        dbqueryFileName = 'dbquery.txt'
        stringIdDataBase = database+"_stringidmap"
        DBH = open(dbqueryFileName,"w")
        DBH.write("select * from generic_idmap;")
        DBH.close()
        dbCommand = '/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root %s < %s' %(stringIdDataBase,dbqueryFileName)
        (commandExecutionStatus, dbString) = commands.getstatusoutput(dbCommand)
        if(commandExecutionStatus == 0):
                dbStringIdList = dbString.split('\n')
                dbStringIdDict = {}
                for idName in dbStringIdList:
                        idNameList = idName.split('\t')
                        dbStringIdDict[idNameList[0]] = idNameList[1]
                return dbStringIdDict
        else:
                print "mysql database not found on machine"
                sys.exit(0)

def readCSVFile(dStr,mStr,file,mOrder):
	dkey = ''
	csvDict = {}
	mValList = []
	#reading the dashboard csv and loading into dict2
        csvFile2 = csv.DictReader(open(file, 'rU'))
	dList = dStr.split('^')
	mList = mStr.split('^')
	mOrderList = mOrder.split(',')
	print mOrderList
	print mList
        for row in csvFile2:
		dkey = ''
		mValList = []
		for d in dList:
			dkey = dkey + "^" + row[d]
		dkey = dkey[1:]
		for m in mOrderList:
			if mMap.has_key(m):
				print mMap[m]
				if ('E' in row[mMap[m]]):
					valList = row[mMap[m]].split('E')
					v1 = round(float(valList[0]), 11)
					mval = v1 * pow(10,float(valList[1]))
				else:
					#getcontext().prec = 13
					mval = float(row[mMap[m]])
					
                		mValList.append(mval)
			else:
				print "Mapping of measures between INSTA and CSV is not proper... exiting..."
				exit(-1)

		if csvDict.has_key(dkey):
			print "ERROR: Duplicate key encountered in CSV %s" %(dkey)
			exit(-1)
        	else:
			csvDict[dkey] = []
			csvDict[dkey] = mValList
	return csvDict

def genTupleKey(tIDKey,strIdMap):
	tKey = ''
	tList = tIDKey.split(',')
	for t in tList:
		try:
			tKey = tKey+"^"+strIdMap[t]
		except:
			tKey = tKey
	return tKey[1:]

def getRE(mListStr):
	print mListStr
	reStr = ''
	mListStr = mListStr.replace('.None','')
	mList = mListStr.split(',')

	for m in mList:
		reStr = reStr+','
		if (m == 'Views' or m == 'UniqueSubscribers'):
			reStr = reStr+'\[.*=(.*),.*\]'
		else:
			reStr = reStr+'(.*)'
	return reStr[1:]		


def getInstaResult(genConf,tupleConf,filterConf):
	configStr = getTupleConfigStats(tupleConf)
	filterStr = getFilterConfigStats(filterConf)
	strIDMapDict = genStringIdMap(dbName)
	
	"""
	if 'PViews' in mListStr:
		pythonCommand = 'python InstaDumper.py  -a timeseries -g %s -m %s -s "%s" -e "%s" -c /opt/tms/xml_schema/cdn_CubeDefinition.xml -d all -i %s -p %s -f \'%s\' -t \'%s\' -x test_result.txt' %(cubeName,mListStr,startTime,endTime,ip,dbPort,filterStr,configStr) 
	else:	
	"""
	pythonCommand = 'python InstaDumper.py  -a get_agg_data -g %s -m %s -s "%s" -e "%s" -c /opt/tms/xml_schema/cdn_CubeDefinition.xml -d all -i %s -p %s -f \'%s\' -t \'%s\' -x test_result.txt' %(cubeName,mListStr,startTime,endTime,ip,dbPort,filterStr,configStr)
        print pythonCommand
        (status,output) = commands.getstatusoutput(pythonCommand)
        outList = output.split('\n')
       	instaDict = {}	
	liVal = []
	flag = 0
	for line in outList:
		if 'NumberOfTuples' in line:
			val = line.split('=')
			print val
			if int(val[-1]) > 0:
				f = open('test_result.txt','r')
				fRes = f.readlines()
				for l in fRes:
					liVal = []
					m = re.search('^tuples,(.*)', l)
					if m is not None:
						mOrderGrp = m.group(1)
						mOrder = mOrderGrp.replace('.None','')
						reStr = getRE(mOrder)
						print reStr
						continue
					m = re.search('^\[',l)
                			if m is not None:
						t = re.search('^\[(.*)\]\,(.*)',l)
						tIDKey = (t.group(1).strip('\r\n')).replace(' ','')
						tupleKey = genTupleKey(tIDKey,strIDMapDict)
						mRE = t.group(2).strip('\r\n')
						v = re.search(reStr, mRE)
						for val in v.groups():
							#getcontext().prec = 6
							liVal.append(float(val.strip()))
						instaDict[tupleKey] = liVal
					else:
						continue	
			else:
				print "Insta Dumper did not return any results... exiting..."
				exit(-1)	
	return (instaDict,mOrder)


def main():
        parser = OptionParser(usage="usage: %prog [options] ",version="%prog 1.0")
        parser.add_option("-g", "--genConfFile",
                        action="store",
                        dest="genConf",
                        type="str",
                        help="Generic Config file with generic parameters like start time, end time etc")
        parser.add_option("-t", "--tupleConfigFile",
                        action="store",
                        dest="tupleConf",
                        type="str",
                        help="Config file")
        parser.add_option("-f", "--filterConfigFile",
                        action="store",
                        dest="filterConf",
                        type="str",
                        help="file containing the filter parameters")
        parser.add_option("-c", "--CSVFile",
                        action="store",
                        dest="CSVFile",
                        type="str",
                        help="CSV file to be validated")
        options, args = parser.parse_args()
	
	if(options.genConf is not None and options.CSVFile is not None):
		print "in here"
		csvFile = options.CSVFile
		genConf = options.genConf
		tupleConf = options.tupleConf
		filterConf = options.filterConf

                getConfigStats(options.genConf)
		(instaDict2, mOrder)  = getInstaResult(genConf,tupleConf,filterConf)
		csvDict1 = readCSVFile(dStr,mStr,csvFile,mOrder)
		print csvDict1
		print instaDict2
		print cmp(csvDict1,instaDict2)

		diff = DictDiffer(instaDict2, csvDict1)
		"""
		print "Added:", d.added()
		print "Removed:", d.removed()
		print "Changed:", d.changed()
		print "Unchanged:", d.unchanged()
		"""
		if not diff.added():
			if not diff.removed():
				if not diff.changed():
					print "pass"
				else:
					print "Changed: ",  diff.changed()
			else:
				print "Removed: ", diff.removed()
		else:
			print "Added: ", diff.added()
		
main()
