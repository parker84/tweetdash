from sqlalchemy import create_engine
from decouple import config

ENGINE_PATH = f"postgresql://{config('DB_USER')}:{config('DB_PWD')}@{config('DB_HOST')}/{config('DB')}"
engine = create_engine(ENGINE_PATH)
db_conn = engine.connect()