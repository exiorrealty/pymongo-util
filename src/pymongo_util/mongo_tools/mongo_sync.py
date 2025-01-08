import sys
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Sequence, Tuple, Union

from .util_configs import MongoConfig

try:
    from pymongo import MongoClient, ReturnDocument
    from pymongo.command_cursor import CommandCursor
    from pymongo.cursor import Cursor
    from pymongo.results import (
        DeleteResult,
        InsertManyResult,
        InsertOneResult,
        UpdateResult,
    )
    from pymongo.typings import _DocumentType
except ImportError:  # pragma: no cover
    sys.stderr.write("PyMongo not installed, run pip install pymongo")
    raise


class MongoCollectionBaseClass:
    def __init__(
        self,
        mongo_client: MongoClient,
        database: str,
        collection: str,
        soft_delete: bool = MongoConfig.META_SOFT_DEL,
    ) -> None:
        self.client = mongo_client
        self.database = database
        self.collection = collection
        self.soft_delete = soft_delete

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(database={self.database}, collection={self.collection})"

    def insert_one(self, data: Dict) -> InsertOneResult:
        """
        The function is used to inserting a document to a collection in a Mongo Database.
        :param data: Data to be inserted
        :return: Insert Result (Refer to Pymongo Documentation for more details)
        """
        database_name = self.database
        collection_name = self.collection
        db = self.client[database_name]
        collection = db[collection_name]
        return collection.insert_one(data)

    def insert_many(self, data: list) -> InsertManyResult:
        """
        The function is used to inserting multiple documents to a collection in a Mongo Database.
        :param data: List of Data to be inserted. Contents of the list must be of mutable mapping (dict)
        :return: Insert Result (Refer to Pymongo Documentation for more details)
        """
        database_name = self.database
        collection_name = self.collection
        db = self.client[database_name]
        collection = db[collection_name]
        return collection.insert_many(data)

    def find(
        self,
        query: dict,
        filter_dict: dict | None = None,
        sort: Union[None, str, Sequence[Tuple[str, Union[int, str, dict]]]] = None,
        skip: int = 0,
        limit: int | None = None,
    ) -> Cursor:
        """
        The function is used to query documents from the collection
        :param query: a mongo query object or dictionary
        :param (Optional) filter_dict: a dictionary with keys from mongo collection.
                If nothing is passed, it defaults to {"_id": 0}
        :param (Optional) sort: List of tuple with key and direction. [(key, -1), ...]
        :param (Optional) skip: Skip Number
        :param (Optional) limit: Limit Number
        :return: A mongo cursor
        """
        sort = sort or []
        if filter_dict is None:
            filter_dict = {"_id": 0}
        database_name = self.database
        collection_name = self.collection
        db = self.client[database_name]
        collection = db[collection_name]
        if len(sort) > 0:
            cursor = (
                collection.find(
                    query,
                    filter_dict,
                )
                .sort(sort)
                .skip(skip)
            )
        else:
            cursor = collection.find(
                query,
                filter_dict,
            ).skip(skip)
        if limit:
            cursor = cursor.limit(limit)
        return cursor

    def find_one(self, query: dict, filter_dict: dict | None = None) -> dict | None:
        """
        The function is used to query documents from the collection
        :param query: a mongo query object or dictionary
        :param (Optional) filter_dict: a dictionary with keys from mongo collection.
                If nothing is passed, it defaults to {"_id": 0}
        :return: document or None
        """
        database_name = self.database
        collection_name = self.collection
        if filter_dict is None:
            filter_dict = {"_id": 0}
        db = self.client[database_name]
        collection = db[collection_name]
        return collection.find_one(query, filter_dict)

    def update_one(
        self,
        query: dict,
        data: dict,
        upsert: bool = False,
        strategy: str = "$set",
    ) -> UpdateResult:
        """
        This function updates a mongo document.
        :param query: a mongo query dictionary
        :param strategy: update strategy (refer mongo documentation). Important note: strategy only supports flat data.
        :param upsert: Setting true inserts data if the query does not match
        :param data: data to be updated with. The behaviour is as per strategy that is passed.
        :return: UpdateResult (refer mongo documentation)
        """
        database_name = self.database
        collection_name = self.collection
        db = self.client[database_name]
        collection = db[collection_name]
        return collection.update_one(query, {strategy: data}, upsert=upsert)

    def update_to_set(
        self, query: dict, param: str, data: Any, upsert: bool = False
    ) -> UpdateResult:
        """
        This function updates a mongo document's array field. This defaults to the `$addToSet` strategy.
        :param query: a mongo query dictionary
        :param param: the key of array field
        :param upsert: Setting true inserts data if the query does not match
        :param data: data to be updated with. The behaviour is as per strategy that is passed.
        :return: UpdateResult (refer mongo documentation)
        """
        database_name = self.database
        collection_name = self.collection
        db = self.client[database_name]
        collection = db[collection_name]
        return collection.update_one(query, {"$addToSet": {param: data}}, upsert=upsert)

    def update_many(
        self, query: dict, data: dict, upsert: bool = False
    ) -> UpdateResult:
        """
        This function updates multiple mongo documents
        :param query: a mongo query dictionary
        :param data: data to be updated with. The behaviour is as per strategy that is passed.
        :return: UpdateResult (refer mongo documentation)
        """
        database_name = self.database
        collection_name = self.collection
        db = self.client[database_name]
        collection = db[collection_name]
        return collection.update_many(query, {"$set": data}, upsert=upsert)

    def find_and_update(
        self, query: dict, data: dict, upsert: bool = False, strategy: str = "$set",
    ) -> _DocumentType:  # type: ignore[type-var, misc]
        """
        This function finds a document and updates it in a single query
        :param query: a mongo query dictionary
        :param data: data to be updated with. The behaviour is as per strategy that is passed.
        :param upsert: Boolean flag to upsert document, if the query does not match
        :return: Updated document
        """
        database_name = self.database
        collection_name = self.collection
        db = self.client[database_name]
        collection = db[collection_name]
        return collection.find_one_and_update(
            query,
            {strategy: data},
            return_document=ReturnDocument.AFTER,
            upsert=upsert,
        )

    def delete_many(self, query: dict) -> DeleteResult:
        """
        Delete multiple document based on query match
        :param query: a mongo query dictionary
        :return: DeleteResult (refer mongo documentation)
        """
        database_name = self.database
        collection_name = self.collection
        db = self.client[database_name]
        collection = db[collection_name]
        if self.soft_delete:
            self.perform_soft_delete(query)
        return collection.delete_many(query)

    def delete_one(self, query: dict) -> DeleteResult:
        """
        Deletes a mongo document for a given query
        :param query: a mongo query dictionary
        :return: DeleteResult (refer mongo documentation)
        """
        database_name = self.database
        collection_name = self.collection
        db = self.client[database_name]
        collection = db[collection_name]
        if self.soft_delete:
            self.perform_soft_delete(query)
        return collection.delete_one(query)

    def perform_soft_delete(self, query):
        soft_del_query = [
            {"$match": query},
            {
                "$addFields": {
                    "deleted": {
                        "on": datetime.now(timezone.utc).replace(tzinfo=timezone.utc)
                    }
                }
            },
            {
                "$merge": {
                    "into": {
                        "db": f"deleted__{self.database}",
                        "coll": self.collection,
                    },
                }
            },
        ]
        self.aggregate(pipelines=soft_del_query)

    def distinct(self, query_key: str, filter_json: dict | None = None) -> list:
        """
        Finds the distinct values for a specified field across a single collection or view and returns the results in an array.
        :param query_key: The field for which to return distinct values
        :param filter_json: A query that specifies the documents from which to retrieve the distinct values.
        :return:
        """
        database_name = self.database
        collection_name = self.collection
        db = self.client[database_name]
        collection = db[collection_name]
        return collection.distinct(query_key, filter_json)

    def find_count(self, query: Dict) -> Cursor:
        database_name = self.database
        collection_name = self.collection
        db = self.client[database_name]
        collection = db[collection_name]
        return collection.count_documents(query)

    def aggregate(
        self,
        pipelines: list,
        let: Mapping[str, Any] | None = None,
        collation=None,
        allowDiskUse=False,  # noqa NOSONAR
    ) -> CommandCursor[_DocumentType]:
        """
        Perform an aggregation using the aggregation framework on this collection
        :param pipelines: A sequence of data aggregation operations or stages. See the MongoDB Docs for details.
        :param let: Specifies a document with a list of variables. This allows you to improve command readability by separating the variables from the query text.
              comment: Any | None = None,
        :param allowDiskUse: Enables writing to temporary files. When set to True, aggregation stages can write data to the _tmp subdirectory in the dbPath directory.
        :param collation: performs case insensitivity on string comparison and diacritic insensitivity on character comparison.
        :return:
        """
        database_name = self.database
        collection_name = self.collection
        db = self.client[database_name]
        collection = db[collection_name]
        return collection.aggregate(
            pipelines, let=let, collation=collation, allowDiskUse=allowDiskUse
        )
