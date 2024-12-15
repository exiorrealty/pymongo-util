"""
Usage instructions:

1. Create a class and inherit `CollectionBaseClass`

2. Pass mongo_client or any other PyMongo compatible client

Example:
    ```
    class RegisteredPlugins(CollectionBaseClass):
    def __init__(self, project_id=None):
        super().__init__(
            mongo_client,
            database=database,
            collection=collection_name,
            project_id=project_id
        )

    def register_plugin(self, data: dict) -> str:
        resp = self.insert_one(data=data)
        return resp.inserted_id

    def update_registration(self, registered_plugin_id: str, data: dict):
        resp = self.update_one(query={"registered_plugin_id": registered_plugin_id}, data=data)
        return resp.modified_count

    def unregister_plugin(self, registered_plugin_id: str) -> int:
        response = self.delete_one({"registered_plugin_id": registered_plugin_id})
        return response.deleted_count

    def fetch_registered_plugin(self, registered_plugin_id: str) -> dict | None:
        return self.find_one(query={"registered_plugin_id": registered_plugin_id})

    def list_registered_plugins(self, skip: int, limit: int, filters: dict) -> CursorType:
        return self.find(query=filters, skip=skip, limit=limit)
    ```
"""

from .__version__ import __version__
from .mongo_tools.mongo_util import MongoConnect
from .mongo_tools.util_configs import MongoConfig

mongo_obj = MongoConnect()

mongo_client = mongo_obj()

CollectionBaseClass = mongo_obj.get_base_class()

__all__ = ["mongo_client", "CollectionBaseClass", "__version__", "MongoConfig", "RedisConfig"]
