# PyMongo Util

PyMongo Util is a utility package that simplifies interaction with MongoDB using PyMongo.

## Installation

Install the package using pip:

```bash
pip install pymongo-util
```

## Usage Instructions

1. Create a class that inherits `CollectionBaseClass`.
2. Pass `mongo_client` or any other PyMongo compatible client during initialization.


Example
```python
from pymongo_util import mongo_client, CollectionBaseClass

class MyCollection(CollectionBaseClass):
    def __init__(self):
        super().__init__(
            client=mongo_client,
            database='my_database',
            collection='my_collection'
        )

    def insert_document(self, data: dict) -> str:
        result = self.insert_one(data=data)
        return str(result.inserted_id)

    def update_document(self, document_id: str, data: dict) -> int:
        result = self.update_one(query={"_id": document_id}, data=data)
        return result.modified_count

    def delete_document(self, document_id: str) -> int:
        result = self.delete_one(query={"_id": document_id})
        return result.deleted_count

    def find_document(self, document_id: str) -> dict:
        return self.find_one(query={"_id": document_id})

    def list_documents(self, skip: int = 0, limit: int = 10, filters: dict = None):
        return self.find(query=filters or {}, skip=skip, limit=limit)
```

## Configuration

Ensure you have a MongoDB instance running and configure the connection settings in `MongoConfig`.
