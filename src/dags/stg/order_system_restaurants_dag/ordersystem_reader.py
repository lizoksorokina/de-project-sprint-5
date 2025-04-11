from datetime import datetime
from typing import Dict, List
from lib import MongoConnect

class MongoReader:
    def __init__(self, mc: MongoConnect, collection_name: str) -> None:
        self.collection = mc.client().get_collection(collection_name)

    def get_documents(self, load_threshold: datetime, limit: int) -> List[Dict]:
        filter = {'update_ts': {'$gt': load_threshold}}
        sort = [('update_ts', 1)]
        return list(self.collection.find(filter=filter, sort=sort, limit=limit))
