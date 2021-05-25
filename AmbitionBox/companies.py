import json
from pprint import pprint

import pymongo
import requests
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from tqdm import tqdm

client = MongoClient(host="localhost", port=27017)
db = client["personal"]
company_collection = db["companies"]

try:
    company_collection.create_index([("CompanyId", pymongo.ASCENDING)], unique=True)
except Exception as e:
    print(e)

def get_companies(page: int):

    url = "https://www.ambitionbox.com/api/v2/companyListingInfo?tag=forbesglobal2000&page={}&sort=popularity".format(str(page))

    payload={}
    headers = {
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="90", "Google Chrome";v="90"',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://www.ambitionbox.com/list-of-companies?&sort_by=popularity&page=2',
    'sec-ch-ua-mobile': '?0',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    data = response.json()

    with open(f"logs/companies-Fx-{str(page)}.json", "w+") as f:
        json.dump(data, f)

    companies = data["data"]["listing"]

    try:
        company_collection.insert_many(companies, ordered=False, bypass_document_validation=True)
    except BulkWriteError as e:
        pass



for page in tqdm(range(1, 28)):
    get_companies(page)
