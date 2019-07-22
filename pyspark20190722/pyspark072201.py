"""
@author     ：bai.chenghui
@date       ：Created in 2019/7/22 13:07
@description：
@modified By：
@version:     
"""
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local[2]').setAppName('pyspark072201')
sc = SparkContext(conf=conf)
distdata = sc.textFile("file:///E:/IdeaProjects/pyspark01/pyspark20190722/data/sparkdata")
mapRdd = distdata.flatMap(lambda line: line.split(' ')).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).map(
    lambda x: (x[1], x[0])).sortByKey(False).map(lambda x: (x[1], x[0]))
print(mapRdd.collect())
