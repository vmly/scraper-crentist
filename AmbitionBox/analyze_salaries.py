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

res = salary_collection.aggregate([
    {"$match": {"AverageCtc": {"$gt": 25}}},
    {"$sort": {"AverageCtc": -1}}
    ])

sorted_salaries = list(res)

[doc.pop("_id") for doc in sorted_salaries]
with open("logs/morethan25.json", "w+") as f:
    json.dump(sorted_salaries, f)

salaries_df = pd.DataFrame(sorted_salaries)
print(salaries_df[["CompanyName", "MaxCtc", "MinCtc", "AverageCtc","JobProfileName"]])

pd.set_option("display.max_rows", None, "display.max_columns", None)
profiles_count = salaries_df["JobProfileName"].value_counts()
pprint(profiles_count)
'''

salaries_bucketed = salary_collection.aggregate(
    [{"$bucketAuto": {"groupBy": "$MinCtc", "buckets": 50}}]
)
salaries_bucketed = list(salaries_bucketed)
salaries_bucketed = sorted(salaries_bucketed, key= lambda item: float(item["_id"]["min"]))
pprint(salaries_bucketed)
print(len(salaries_bucketed))

manual_buckets = salary_collection.aggregate([
    {"$bucket": {
        "groupBy": "$MinCtc",
        "boundaries": [i for i in range(0, 100)],
        "default": "Other",
        "output": {
            "count": {"$sum": 1}
        }
    }}
])
print(list(manual_buckets))
'''