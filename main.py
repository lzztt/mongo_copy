# copy mongo docs from one collection to another
from pymongo import MongoClient

SRC_URL = "source_db"
SRC_DB = "source_db"
SRC_COLL = "source_coll"

DEST_URL = "dest_db"
DEST_DB = "dest_db"
DEST_COLL = "dest_coll"


def connect(uri) -> MongoClient:
    try:
        client = MongoClient(
            host=uri,
            unicode_decode_error_handler="ignore",
        )
        return client
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None


with open("doc_ids.txt") as f:
    doc_ids = [line.strip() for line in f]
    src = connect(SRC_DB)[SRC_DB][SRC_COLL]
    dest = connect(DEST_DB)[DEST_DB][DEST_COLL]

    for doc_id in doc_ids:
        doc = src.find_one({"_id": doc_id})
        if doc:
            dest.insert_one(doc)
            print(f"copied: {doc_id}")
        else:
            print(f"not found: {doc_id} not found")
