import datetime
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
import os
import shutil
import numpy as np
import dateutil.parser as dparser
########path#############
dir = '/home/hadoop/Desktop/part1'
########## init##########
os.environ["PYSPARK_PYTHON"]="python3"
os.environ["PYSPARK_DRIVER_PYTHON"]="python3"
conf = SparkConf().setMaster("local")
sc = SparkContext(conf=conf)
sqlContext =SQLContext(sc)

def convertTimeToCalendar(time):
    weekday = datetime.datetime.fromtimestamp(np.long(time/1000)).strftime("%w")
    weekofyear = datetime.datetime.fromtimestamp(np.long(time/1000)).strftime("%W")
    return weekofyear+"_"+weekday

def AppendString(a, b):
    return str(a) + "_" + str(b)

def main():
    pageviewdata = sqlContext.read.parquet("/home/hadoop/Downloads/{parquet_logfile_at_20h_55.snap}").repartition(1)
    pageviewdata.registerTempTable("pageview")
    sqlpageview = sqlContext.sql("select guid,time_group.time_create from pageview")

    groupByGuid = sqlpageview.rdd.map(lambda x : (x[0], convertTimeToCalendar(x[1])))
    groupByGuidAndCalendars = groupByGuid.map(lambda x : (AppendString(x[0], x[1]), 1))
    print(groupByGuidAndCalendars.take(10))

    countByGuidAndCalendars = groupByGuidAndCalendars.reduceByKey(lambda a, b : a + b).\
        map(lambda x : (x[0].split("_")[0] + "_" + x[0].split('_')[1], AppendString(x[0].split("_")[2],x[1])) ).\
        groupByKey().map(lambda x:(x[0],list(x[1]))).collect()
    print(countByGuidAndCalendars[1:10])
    if os.path.exists(dir):
        shutil.rmtree(dir)



if __name__ == '__main__':
    main()