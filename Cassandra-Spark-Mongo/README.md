#### Prerequisites
- Spark installed and configured (spark-1.6.1-bin-hadoop2.4 and later)
- Python 2.7
- Java Development Kit (JDK)
- MongoDB
- Cassandra 3

##### Environmental variable settings
* HADOOP_HOME = directory\to\hadoop
* PATH = %PATH%;%SPARK_HOME%\bin;%HADOOP_HOME%\bin;C:\Program Files\MongoDB\Server\3.2\bin;C:\cassandra\bin

##### Download and install mongo-hadoop
* Download [mongo-hadoop](git clone https://github.com/mongodb/mongo-hadoop.git)
* Go to the pymongo-spark directory of the project and install
```
	cd mongo-hadoop/spark/src/main/python
	python setup.py install
```
* May need to set the environmental variable
```
	PYTHONPATH = %PYTHONPATH%";directory\to\mongo-hadoop\spark\src\main\python
```
* Install pymongo on each machine in your Spark cluster.
```	
	pip install pymongo
```
* Download 'mongo-hadoop-spark.jar' from [Maven Central Repository](http://search.maven.org/).


##### Usage
* run `mongod` from command line to start mongodb
* run `bin/cassandra -f` from command line to start cassandra
* create a keyspace -- a namespace of tables.
```
CREATE KEYSPACE mykeyspace
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
```
* authenticate to the new keyspace:
```
USE mykeyspace;
```
* create a `transactions` table:
```
CREATE TABLE users (
  user_id int,
  product_id int,
  score int
);
```
* Copy the data from the CSV file into the `transactions` table
```
COPY transactions from 'data2.csv';
```
* cassandra_mongo.py loads the data from `transactions`, calculates the recommendations and saves the result into MongoDB.
```
spark-submit \
    --packages TargetHolding/pyspark-cassandra:<version> \
    --conf spark.cassandra.connection.host=your,cassandra,node,names \
    --driver-class-path mongo-hadoop-spark.jar
    cassandra_mongo.py
``` 
* check the collection in mongodb
```
	mongo
	use test_database
	db.recomm_per_user.count()
	db.recomm_per_item.count()
```


### Useful links:

 * [Environmental virable settings](http://stackoverflow.com/questions/33391840/getting-spark-python-and-mongodb-to-work-together) Getting spark, python and mongodb to work together

 * [MongoDB Connector for Hadoop](https://github.com/mongodb/mongo-hadoop)

 * [PySpark Cassandra](https://github.com/TargetHolding/pyspark-cassandra)


##### Environmental variable setting for Spark

* JAVA_HOME = the value is JDK path.
* PATH =  %PATH%;'%JAVA_HOME%\bin'
* PYTHONPATH = python home directory plus scripts directory inside the python home directory, separated by semicolon.
* PATH = %PATH%;'%PYTHONPATH%'
* PYSPARK_DRIVER_PYTHON = ipython
* PYSPARK_DRIVER_PYTHON_OPTS = notebook
