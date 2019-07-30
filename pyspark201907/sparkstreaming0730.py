import sys
if __name__=='__main__':
    if len(sys.argv) != 2:
        print("Usage sparkstreaming0730.py <directory>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setMaster('localhost[2]').setAppName('streaming0730')
    sc = SparkContext(conf)
    ssc = StreamingContext(sc, 5)
    lines =ssc.textFileStream(sys.argv[1])
    counts = lines.flatMap(lambda line: line,split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a,b: a+b)
    countes.pprint()
    ssc.start()
    ssc.awaittermination()
