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


def demo1():
    map_rdd = distdata.flatMap(lambda line: line.split(' ')).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).map(
        lambda x: (x[1], x[0])).sortByKey(False).map(lambda x: (x[1], x[0]))
    # print(map_rdd.collect())
    # topN
    print(map_rdd.take(2))
    sc.stop()


def demo2():
    # 求均值
    data = [1, 2, 3, 4, 5]
    dist_data = sc.parallelize(data)
    sums = dist_data.reduce(lambda x, y: x+y)
    avgs = sums/len(data)
    print(avgs)


def main():
    # demo1()
    demo2()


if __name__ == '__main__':
    main()
