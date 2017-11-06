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
    weekday = datetime.datetime.fromtimestamp(np.long(time)).strftime("%w")
    weekofyear = datetime.datetime.fromtimestamp(time).strftime("%W")
    return weekofyear+"_"+weekday

def GuidAndWeeks(a, b):
    return a.toString + "_" + b

def main():
    pageviewdata = sqlContext.read.parquet("/home/hadoop/Downloads/{parquet_logfile_at_20h_35.snap}").repartition(1)
    pageviewdata.registerTempTable("pageview")
    sqlpageview = sqlContext.sql("select guid,time_group.time_create from pageview")

    groupByGuid = sqlpageview.rdd.map(lambda x : (x(0), convertTimeToCalendar(x(1))))
    groupByGuidAndCalendars = groupByGuid.map(lambda x : (GuidAndWeeks(x[0], x[1]), 1))
    countByGuidAndCalendars = groupByGuidAndCalendars.reduceByKey(lambda a, b : a + b).map(lambda x : (x._1.split("_")(0) + "_" + x._1.split('_')(1), (x._1.split("_")(2) + "_" + x._2.toString))).groupByKey()
    if os.path.exists(dir):
        shutil.rmtree(dir)
    countByGuidAndCalendars.saveAsTextFile(dir)


if __name__ == '__main__':
    main()