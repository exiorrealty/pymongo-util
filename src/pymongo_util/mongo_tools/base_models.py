from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class AGGridFilterModel(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)

    group_keys: list = []
    row_group_cols: list = []
    sort_model: list = []
    filter_model: dict = {}
    value_cols: list = []
    pivot_cols: list = []
    pivot_mode: str | None = None
    quick_filter: bool = False
    flag_columns: list = []
    flag_filters: list = []


class AGGridTableRequest(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)

    page: int = 1
    records: int = 50
    filters: AGGridFilterModel | None = None
    global_filters: dict = {}
    start_row: int = 0
    end_row: int = 100


__all__ = ["AGGridTableRequest", "AGGridFilterModel"]
