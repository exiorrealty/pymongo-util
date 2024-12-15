""" Mongo DB utility
All definitions related to mongo db is defined in this module
"""

import sys
from typing import Type

try:
    from pymongo import MongoClient
except ImportError:  # pragma: no cover
    sys.stderr.write("PyMongo not installed, run pip install pymongo")
    raise

from . import mongo_sync
from .util_configs import MongoConfig


class MongoConnect:
    def __init__(self, client: MongoClient | None = None) -> None:
        self.client = client or MongoClient(MongoConfig.MONGO_URI, connect=False)

    def __call__(self, *args, **kwargs):  # type: ignore
        return self.client

    @staticmethod
    def get_base_class() -> Type[mongo_sync.MongoCollectionBaseClass]:
        return mongo_sync.MongoCollectionBaseClass


class MongoStageCreator:
    @staticmethod
    def add_stage(stage_name: str, stage: dict) -> dict:
        return {stage_name: stage}

    def projection_stage(self, stage: dict) -> dict:
        return self.add_stage("$project", stage)

    def match_stage(self, stage: dict) -> dict:
        return self.add_stage("$match", stage)

    def lookup_stage(self, stage: dict) -> dict:
        return self.add_stage("$lookup", stage)

    def unwind_stage(self, stage: dict) -> dict:
        return self.add_stage("$unwind", stage)

    def group_stage(self, stage: dict) -> dict:
        return self.add_stage("$group", stage)

    def add_fields(self, stage: dict) -> dict:
        return self.add_stage("$addFields", stage)

    def sort_stage(self, stage: dict) -> dict:
        return self.add_stage("$sort", stage)
