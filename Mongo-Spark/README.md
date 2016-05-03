# pymongo-spark installation

#### Prerequisites
- Spark installed and configured (spark-1.4.1-bin-hadoop2.4 and later)
- Python 2.7
- Java Development Kit (JDK)
- MongoDB


##### Environmental variable setting for Spark

* JAVA_HOME = the value is JDK path.
* PATH =  %PATH%;‘%JAVA_HOME%\bin’
* PYTHONPATH = python home directory plus scripts directory inside the python home directory, separated by semicolon.
* PATH = %PATH%;‘%PYTHONPATH%’
* PYSPARK_DRIVER_PYTHON = ipython
* PYSPARK_DRIVER_PYTHON_OPTS = notebook

##### Environmental variable setting for MongoDB-Spark
* HADOOP_HOME = directory\to\hadoop
* PATH = %PATH%;%SPARK_HOME%\bin;%HADOOP_HOME%\bin;C:\Program Files\MongoDB\Server\3.2\bin

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
* run `mongod` in `cmd` to start background management operations
* spark-submit --driver-class-path mongo-hadoop-spark.jar rdd_loading.py


### Useful links:

 * [Environmental virable settings](http://stackoverflow.com/questions/33391840/getting-spark-python-and-mongodb-to-work-together) Getting spark, python and mongodb to work together

 * [MongoDB Connector for Hadoop](https://github.com/mongodb/mongo-hadoop)


