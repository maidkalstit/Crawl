import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
ENV_FILE = BASE_DIR / ".env"

if ENV_FILE.exists():
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=ENV_FILE)

def get_env_required(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise KeyError(f"[CRITICAL] Thừa hành cấu hình thất bại: Thiếu biến môi trường bắt buộc '{key}'")
    return val

class Settings:
    COINGECKO_API_KEY: str = os.getenv("COINGECKO_API_KEY", "")

    # MongoDB Config - Khóa chặt bảo mật, không fallback text thô
    MONGO_USER: str = get_env_required("MONGO_USER")
    MONGO_PASSWORD: str = get_env_required("MONGO_PASSWORD")
    MONGO_HOST: str = os.getenv("MONGO_HOST", "localhost")
    MONGO_PORT: int = int(os.getenv("MONGO_PORT", 27019))
    MONGO_DB_NAME: str = os.getenv("MONGO_DB_NAME", "coingecko_staging")
    
    @property
    def mongo_uri(self) -> str:
        return f"mongodb://{self.MONGO_USER}:{self.MONGO_PASSWORD}@{self.MONGO_HOST}:{self.MONGO_PORT}/"

    # PostgreSQL Config - Bắt buộc đọc từ biến môi trường
    POSTGRES_USER: str = get_env_required("POSTGRES_USER")
    POSTGRES_PASSWORD: str = get_env_required("POSTGRES_PASSWORD")
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", 5439))
    POSTGRES_DB_NAME: str = os.getenv("POSTGRES_DB_NAME", "coingecko_warehouse")

    @property
    def postgres_uri(self) -> str:
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB_NAME}"

    # VÁ LỖI BUG LOGSTASH HOST: Tách biệt cấu hình độc lập hoàn toàn với Postgres
    LOGSTASH_HOST: str = os.getenv("LOGSTASH_HOST", "localhost")
    LOGSTASH_PORT: int = int(os.getenv("LOGSTASH_PORT", 5049))

settings = Settings()