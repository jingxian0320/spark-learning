from pyspark import SparkContext, SparkConf

import pymongo_spark

# Important: activate pymongo_spark.
pymongo_spark.activate()


def main():
    conf = SparkConf().setAppName("pyspark_test")
    sc = SparkContext(conf=conf)

    rdd = sc.mongoRDD('mongodb://localhost:27017/test_database.transactions_small')
    print (rdd.collect())


if __name__ == '__main__':
    main()


