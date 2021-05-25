import time

from joblib import Parallel, delayed
from pymongo import MongoClient
from tqdm.std import tqdm

from salary import get_salary
from utils import get_batch_docs, to_snake_case, update_flag_batch

client = MongoClient(host="localhost", port=27017)
db = client["personal"]
company_collection = db["companies"]

def run_salaries(company_collection, batch_size=32, designation="Software Engineer"):

    search_flag = "salary_{}".format(to_snake_case(designation))

    batches = company_collection.count_documents({}) // batch_size + 1
    print(batches)


    for i in tqdm(range(batches)):
        docs = get_batch_docs(
            company_collection, batch_size, search_flag, additional_query={}
        )
        print([doc["Name"] for doc in docs])
        if not docs:
            print("GET ALL ITEMS : COMPLETED")
            break

        results_list = Parallel(n_jobs=batch_size, prefer="threads")(
            delayed(get_salary)(company, designation) for run_idx, company in enumerate(docs)
        )

        update_flag_batch(company_collection, search_flag, "completed", [doc["_id"] for doc in docs])
        # break
        time.sleep(0.3)

run_salaries(company_collection)