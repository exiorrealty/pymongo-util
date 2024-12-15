from pydantic import Field
from pydantic_settings import BaseSettings


class _MongoConfig(BaseSettings):
    MONGO_URI: str | None = Field(default=None)
    META_SOFT_DEL: bool = Field(default=True)


MongoConfig = _MongoConfig()

__all__ = ["MongoConfig"]
