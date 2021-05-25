import json
from pprint import pprint
import re

import pymongo
import requests
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from tqdm import tqdm

client = MongoClient(host="localhost", port=27017)
db = client["personal"]
company_collection = db["companies"]

company_sizes = company_collection.aggregate(
    [
        {"$group": {"_id": "$TotalEmployees", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
)
print("\n - company_sizes")
pprint(list(company_sizes))

reviews = company_collection.aggregate(
    [{"$bucketAuto": {"groupBy": "$Rating", "buckets": 10}}]
)
print("\n - reviews")
pprint(list(reviews))

num_salaries = company_collection.aggregate(
    [{"$bucketAuto": {"groupBy": "$CompanySalaries", "buckets": 10}}]
)
print("\n - num_salaries")
pprint(list(num_salaries))