import json
from pprint import pprint
import re

import pymongo
import requests
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from tqdm import tqdm
import pandas as pd


client = MongoClient(host="localhost", port=27017)
db = client["personal"]
company_collection = db["companies"]
salary_collection = db["salaries"]

docs = salary_collection.find({})

sample = {
    "AverageCtc" : "4.4",
    "MinCtc" : "3.0",
    "MaxCtc" : "7.5",
    "DataPoints" : "7023",
    "AverageExperience" : "3.39",
    "MinExperience" : "0",
    "MaxExperience" : "11",
}
keys = list(sample.keys())
for doc in docs:
    for key in keys:
        try:
            doc.update({key : float(doc.get(key, 0))})
        except Exception as e:
            print(doc) 
    salary_collection.update_one(
        {"_id": doc["_id"]},
        {"$set": doc})