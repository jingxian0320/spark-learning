import csv
import json
import pandas as pd
import sys, getopt, pprint
from pymongo import MongoClient

#CSV to JSON Conversion
csvfile = open('data2.csv', 'r')
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
transaction = db.transactions_small.find().limit(10)
for record in transaction:
    print record

print ("%d transaction records are found in the database"%db.transactions_small.count())