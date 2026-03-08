from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://odata:odata_secret@localhost:5432/odata_1c"
    server_host: str = "0.0.0.0"
    server_port: int = 8080

    model_config = {"env_prefix": "", "case_sensitive": False}


settings = Settings()
