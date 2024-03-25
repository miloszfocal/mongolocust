from locust import between

from mongo_user import MongoUser, mongodb_task
from settings import DEFAULTS
from bson import ObjectId
from pymongo.write_concern import WriteConcern

# hacky overriding HEARTBEAT_VALUE
###############################################
from locust import runners
runners.HEARTBEAT_LIVENESS = 6
runners.HEARTBEAT_INTERVAL = 2
###############################################

import pymongo
import random

# number of cache entries for queries
NAMES_TO_CACHE = 1000


class MongoSampleUser(MongoUser):
    """
    Generic sample mongodb workload generator
    """
    # no delays between operations
    wait_time = between(0.0, 0.0)

    def __init__(self, environment):
        super().__init__(environment)
        self.name_cache = []

    def generate_new_document(self):
        """
        Generate a new sample document
        """
        try:
            # document = {
            #     "oos_id": self.faker.pyint(min_value=100000000, max_value=900000000000),
            #     "stage": self.faker.pystr(),
            #     "is_active": self.faker.pybool(),
            #     "store_id": self.faker.pyint(min_value=1000, max_value=10000),
            #     "department": self.faker.pystr(),
            #     "aisle": self.faker.pystr(),
            #     "location_key": self.faker.pystr(),
            #     "start_time": self.faker.iso8601(),
            #     "end_time": self.faker.iso8601(),
            #     "max_end_time": self.faker.iso8601(),
            #     "valid_at": self.faker.iso8601(),
            #     "created_at": self.faker.iso8601(),
            #     "updated_at": self.faker.iso8601(),
            #     "date": self.faker.date(),
            #     "oos_event": [
            #         {
            #             "stage": self.faker.pystr(),
            #             "created_at": self.faker.iso8601()
            #         },
            #         {
            #             "stage": self.faker.pystr(),
            #             "created_at": self.faker.iso8601()
            #         },
            #         {
            #             "stage": self.faker.pystr(),
            #             "created_at": self.faker.iso8601()
            #         }
            #     ],
            #     "bbox_id": self.faker.pyint(min_value=100000, max_value=999999999),
            #     "state": self.faker.pystr(),
            #     "reason": self.faker.pystr(min_chars=5, max_chars=10),
            #     "source": self.faker.pystr(),
            #     "product": {
            #         "name": self.faker.pystr(),
            #         "brand": self.faker.company(),
            #         "image": self.faker.url(),
            #         "price": self.faker.pyfloat(positive=True),
            #         "upc": self.faker.ean(length=13),
            #         "item_number": self.faker.pystr(),
            #         "supplier": self.faker.pystr(),
            #         "created_at": self.faker.date(),
            #         "updated_at": self.faker.date(),
            #         "inventory_level": self.faker.pyfloat(positive=True, min_value=1, max_value=100),
            #         "product_dimensions": {
            #             "height": self.faker.pyfloat(positive=True, min_value=1, max_value=50),
            #             "depth": self.faker.pyfloat(positive=True, min_value=1, max_value=50),
            #             "width": self.faker.pyfloat(positive=True, min_value=1, max_value=50)
            #         },
            #         "case_pack": [
            #             {
            #                 "upc": self.faker.ean(length=13),
            #                 "case_pack": self.faker.pyfloat(positive=True, min_value=1, max_value=10),
            #                 "case_uom": self.faker.pystr(),
            #                 "created_at": self.faker.iso8601(),
            #                 "pack_size": self.faker.pyint(),
            #                 "pack_number": self.faker.pystr()
            #             },
            #             {
            #                 "upc": self.faker.ean(length=13),
            #                 "case_pack": self.faker.pyfloat(positive=True, min_value=1, max_value=10),
            #                 "case_uom": self.faker.pystr(),
            #                 "created_at": self.faker.iso8601(),
            #                 "pack_size": self.faker.pyint(),
            #                 "pack_number": self.faker.pystr()
            #             },
            #             {
            #                 "upc": self.faker.ean(length=13),
            #                 "case_pack": self.faker.pyfloat(positive=True, min_value=1, max_value=10),
            #                 "case_uom": self.faker.pystr(),
            #                 "created_at": self.faker.iso8601(),
            #                 "pack_size": self.faker.pyint(),
            #                 "pack_number": self.faker.pystr()
            #             }
            #         ]
            #     }
            # }

            document = {
                "oos_id": self.faker.pyint(min_value=100000000, max_value=9000000000),
                "stage": self.faker.pystr(),
                "is_active": self.faker.pybool(),
                "store_id": self.faker.pyint(min_value=1000, max_value=10000),
                "department": self.faker.pystr(),
                "aisle": self.faker.pystr(),
                "location_key": self.faker.pystr(),
                "start_time": self.faker.iso8601(),
                "oos_event": [
                    {
                        "stage": self.faker.pystr(),
                        "created_at": self.faker.iso8601()
                    }
                ],
                "bbox_id": self.faker.pyint(min_value=100000, max_value=99999999),
                "state": self.faker.pystr(),
                "reason": self.faker.pystr(min_chars=1, max_chars=5),
                "source": self.faker.pystr(),
                "product": {
                    "name": self.faker.pystr(),
                    "image": self.faker.url(),
                    "supplier": self.faker.pystr(),
                    "created_at": self.faker.date(),
                    "updated_at": self.faker.date(),
                    "inventory_level": self.faker.pyfloat(positive=True, min_value=1, max_value=20),
                    "product_dimensions": {
                        "height": self.faker.pyfloat(positive=True, min_value=1, max_value=20),
                        "depth": self.faker.pyfloat(positive=True, min_value=1, max_value=20),
                        "width": self.faker.pyfloat(positive=True, min_value=1, max_value=20)
                    },
                    "case_pack": [
                        {
                            "upc": self.faker.ean(length=13),
                            "case_pack": self.faker.pyfloat(positive=True, min_value=1, max_value=10),
                            "created_at": self.faker.iso8601(),
                            "pack_size": self.faker.pyint(),
                            "pack_number": self.faker.pystr()
                        }
                    ]
                }
            }
        except Exception:
            # jest problem z FAKERem niestety i generacja dokumentu z jakiegos powodu chuj wie jakiego ?
            print("PROBLEM Z FAKEREM")
            document = {}

        return document

    def on_start(self):
        """
        Executed every time a new test is started - place init code here
        """
        # prepare the collection
        # WITHOUT INDEXES
        # index1 = pymongo.IndexModel([('first_name', pymongo.ASCENDING), ("last_name", pymongo.DESCENDING)],
        #                             name="idx_first_last")
        indexes = []
        self.collection, self.collection_secondary = self.ensure_collection(DEFAULTS['COLLECTION_NAME'],
                                                                            indexes=indexes)
        self.name_cache = []

    @mongodb_task(weight=int(DEFAULTS['INSERT_WEIGHT']))
    def insert_single_document(self):
        document = self.generate_new_document()

        inserted_result = self.collection.insert_one(document)

        print("*******************")
        print("WRITE_CONCERN")
        print(self.db.write_concern)
        print("*******************")

        if len(self.name_cache) < NAMES_TO_CACHE:
            self.name_cache.append(inserted_result.inserted_id)
        else:
            if random.randint(0, 5) == 0:
                self.name_cache[random.randint(0, len(self.name_cache) - 1)] = inserted_result.inserted_id

    @mongodb_task(weight=int(DEFAULTS['FIND_WEIGHT']))
    def find_document(self):
        if not self.name_cache:
            # at least one insert needs to happen
            return

        # find a random document using an index
        cached_names = random.choice(self.name_cache)
        result = self.collection.find_one({'_id': ObjectId(cached_names)})
        # print(f"FIND RESULT: {result}")

        # if self.name_cache:
        #     print("**************")
        #     print(self.name_cache)
        #     print("**************")
