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
salary_collection = db["salaries"]

try:
    salary_collection.create_index([("SalaryId", pymongo.ASCENDING)], unique=True)
except Exception as e:
    print(e)

def get_salary(company, designation):

    url = "https://www.ambitionbox.com/api/salaries/{}-salaries?page=1&designation={}".format(company["UrlName"], designation)

    payload={}
    headers = {
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="90", "Google Chrome";v="90"',
    'Accept': '*/*',
    'Referer': 'https://www.ambitionbox.com/salaries/{}-salaries'.format(company["UrlName"]),
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua-mobile': '?0',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
    'Content-Type': 'application/json',
    'Cookie': 'PHPSESSID=db5bcf7f4c42847a5860873f484ce2c6'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    response_data = response.json()
    data = response_data["salaryData"]["JobProfiles"]

    for salary in data:
        salary.update({
            "CompanyId": company["CompanyId"],
            "CompanyName": company["Name"],
            "CompanyUrlName": company["UrlName"],
            "SalaryId": "{}-{}".format(str(salary["CompanyId"]), str(salary["JobProfileId"])) 
        })

    try:
        if not data:
            return None
        result = salary_collection.insert_many(data, ordered=False, bypass_document_validation=True)
        return result.inserted_ids
    except BulkWriteError as e:
        pass
    return None


if __name__ == "__main__":

    (url_name, designation) = ("hcl-technologies", "Software Engineer")
    company = company_collection.find_one({"UrlName": url_name})
    get_salary(company, designation)

