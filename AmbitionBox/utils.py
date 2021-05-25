from pymongo import collection
from collections import OrderedDict



def get_batch_docs(
    collection: collection,
    batch_size: int,
    operation_name: str,
    additional_query: dict = {},
) -> list:
    current_flag = "flags.{}".format(operation_name)
    find_query = {current_flag: {"$exists": False}}
    find_query.update(additional_query)
    docs = list(collection.find(find_query, limit=batch_size))
    if docs:
        doc_ids = [doc["_id"] for doc in docs]
        start_ack = update_flag_batch(
            collection, operation_name, "started", doc_ids, doc_key="_id"
        )

    return docs


def update_flag_batch(
    collection: collection,
    operation_name: str,
    flag_state: str,
    doc_ids: list,
    doc_key: str = "_id",
) -> bool:
    # TODO : Update Date - lastModified
    current_flag = "flags.{}".format(operation_name)

    update_result = collection.update_many(
        {doc_key: {"$in": doc_ids}}, {"$set": {current_flag: flag_state}}
    )
    acknowledgement = update_result.modified_count == len(doc_ids)
    return acknowledgement


def reset_unprocessed(from_collection, operation_name):
    # RUN Final Check
    current_flag = "flags.{}".format(operation_name)
    completed_count = from_collection.count_documents({current_flag: "completed"})
    all_count = from_collection.count_documents({})

    # Reset unfinished documents
    if all_count != completed_count:
        reset_result = from_collection.update_many(
            {current_flag: "started"}, {"$unset": {current_flag: ""}}
        )
    return {
        "completed": all_count == completed_count,
        "unprocessed": all_count - completed_count,
        "total_count": all_count,
    }


def unique_list(lst):
    """
    Remove duplicates while preserving the order of the list

    Args:
        lst (list): input list

    Returns:
        list: output list
    """
    return list(OrderedDict.fromkeys(lst))

def to_snake_case(text_input):
    text_no_spec_chars = "".join(
        character.lower() if character.isalnum() else " " for character in text_input
    )
    text_underscored = "_".join(text_no_spec_chars.split())
    return text_underscored