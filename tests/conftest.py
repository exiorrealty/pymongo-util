import mongomock
import pytest

from pymongo_util.mongo_tools.mongo_util import MongoConnect


@pytest.fixture(scope="session")
@mongomock.patch(servers=(("server.example.com", 27017),))
def mongo_client():
    client = MongoConnect(client=mongomock.MongoClient())
    BaseClass = client.get_base_class()  # noqa NOSONAR
    return BaseClass(
        mongo_client=client(), database="mock_data", collection="mock_coll"
    )
