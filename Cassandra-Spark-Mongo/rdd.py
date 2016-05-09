# load rdd from mongodb
# and save back as a copy

from pyspark import SparkContext, SparkConf

import pymongo_spark

# Important: activate pymongo_spark.
pymongo_spark.activate()


def main():
    print ('Start!')
    conf = SparkConf().setAppName("pyspark_test")
    print (1)
    sc = SparkContext(conf=conf)
    print (2)
    rdd = sc.mongoRDD('mongodb://localhost:27017/test_database.transactions')
    print (3)
    rdd.saveToMongoDB('mongodb://localhost:27017/test_database.transactions_copy')
    print ('Completed!')
if __name__ == '__main__':
    main()


