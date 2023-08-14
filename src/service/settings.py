
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_name: str = "Awesome API"
    db_url: str
    db_sync_url: str

    class Config:
        env_file = ".env"
