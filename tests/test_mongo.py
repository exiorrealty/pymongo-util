import json
import pathlib

MOCK_DATA_PATH = pathlib.Path("tests/mocks/data.json")


def test_insert_one_find_one(mongo_client):
    mongo_client.insert_one({"first_name": "Evania"})
    data = mongo_client.find_one({"first_name": "Evania"})
    assert data["first_name"] == "Evania"


def test_insert_many_find_one(mongo_client):
    with open(MOCK_DATA_PATH) as fp:
        data = json.load(fp)
    mongo_client.insert_many(data)

    data = mongo_client.find_one({"first_name": "Ardella"})
    assert data["first_name"] == "Ardella"


def test_update_one(mongo_client):
    query = {"first_name": "Evania", "id": {"$exists": False}}
    res = mongo_client.update_one(
        query=query,
        data={
            "id": 101,
            "last_name": "Riccio",
            "email": "ericcio26@bigcartel.com",
            "gender": "Female",
            "ip_address": "67.210.212.155",
        },
    )
    data = mongo_client.find_one({"id": 101})
    assert data["first_name"] == "Evania"
    assert res.matched_count == 1
    assert res.modified_count == 1


def test_find(mongo_client):
    data = mongo_client.find({"first_name": "Evania"})
    assert len(list(data)) == 2


def test_update_many(mongo_client):
    query = {"id": {"$lte": 10}}
    res = mongo_client.update_many(
        query=query,
        data={"change_arr": ["change"]},
    )
    assert res.matched_count == 10
    assert res.modified_count == 10
    data = mongo_client.find({"change_arr": {"$exists": True}})
    assert len(list(data)) == 10


def test_update_to_set(mongo_client):
    query = {"id": 1}
    res = mongo_client.update_to_set(
        query=query,
        param="change_arr",
        data="change2",
    )
    assert res.matched_count == 1
    assert res.modified_count == 1
    data = mongo_client.find_one(query)
    assert data["change_arr"] == ["change", "change2"]


def test_find_and_update(mongo_client):
    query = {"id": 1}
    updated_doc = mongo_client.find_and_update(
        query=query,
        data={"last_name": "Joze"},
    )
    assert updated_doc["last_name"] == "Joze"


def test_distinct(mongo_client):
    query = {"id": {"$lte": 10}}
    res = mongo_client.distinct("gender", filter_json=query)
    gender_list = ["Male", "Female", "Genderfluid", "Non-binary"]
    assert all(i in gender_list for i in res)


def test_delete_one(mongo_client):
    query = {"id": 101}
    res = mongo_client.delete_one(query)
    assert res.deleted_count == 1
    data = mongo_client.find_one(query)
    assert data is None


def test_delete_many(mongo_client):
    query = {"change_arr": {"$exists": True}}
    res = mongo_client.delete_many(query)
    assert res.deleted_count == 10


def test_update_upsert(mongo_client):
    query = {"id": 1}
    res = mongo_client.update_one(
        query=query,
        data={
            "id": 1,
            "first_name": "Evania",
            "last_name": "Riccio",
            "email": "ericcio26@bigcartel.com",
            "gender": "Female",
            "ip_address": "67.210.212.155",
        },
        upsert=True,
    )
    data = mongo_client.find_one({"id": 1})
    assert data["first_name"] == "Evania"
    assert res.matched_count == 0
    assert res.modified_count == 0
