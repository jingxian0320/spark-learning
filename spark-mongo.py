# count the number of distinct words in a txt file

import csv
import json
import pandas as pd
import sys, getopt, pprint
from pymongo import MongoClient


#CSV to JSON Conversion
filename = '../data/transactions_sparkdata_small.csv'
csvfile = open(filename, 'r')
reader = csv.DictReader( csvfile,fieldnames= [ "CustomerID", "ProductID", "Score"])

mongo_client=MongoClient() 
db=mongo_client.test_database

db.transaction_small.drop()
header= [ "CustomerID", "ProductID", "Score"]

for each in reader:
    row={}
    for field in header:
        row[field]=each[field]
    db.transactions_small.insert_one(row)

db.collection_names(include_system_collections=False)

transactions = db.transactions_small.find()[:10]
for record in transactions:
    print record

import json
data = transactions.map(lambda x:json.loads(x))
data

#import pymongo
#import pymongo_spark

#mongo_url = 'mongodb://mongo:27017/'

#pymongo_spark.activate()
#mongo_rdd = sc.mongoRDD('mongodb://localhost:27017/test_database.transactions_small')
#rdd.collect()
