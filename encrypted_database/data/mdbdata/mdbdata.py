from typing import List

from bson import ObjectId
from pymongo import MongoClient
from pymongo.database import Database


class MDBData:
    def __init__(self, host: str, database: str) -> None:
        self.mdb: Database = MongoClient(host)[database]

    def insert_one(self, table: str, document):
        return self.mdb[table].insert_one(document)

    def insert_many(self, table: str, documents):
        return self.mdb[table].insert_many(documents)

    def fetch(self, table: str, ids: List[ObjectId]):
        return self.mdb[table].find({"_id": {"$in": ids}})
