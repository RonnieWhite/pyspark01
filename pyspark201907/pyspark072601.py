"""
@author     ：bai.chenghui
@date       ：Created in 2019/7/26 17:00
@description：
@modified By：
@version:     
"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Python Spark SQL basic example")\
    .config("spark.some.config.option", "some-value").getOrCreate()
df = spark.read.json('file:///E:/IdeaProjects/pyspark01/pyspark201907/data/people.json')
df.createOrReplaceTempView('person')
sqlDF = spark.sql("SELECT * FROM person")
sqlDF.show()
