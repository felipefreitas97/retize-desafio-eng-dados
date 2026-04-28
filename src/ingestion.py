import pandas as pd
from sqlalchemy import create_engine, text
from pydantic import BaseModel, ValidationError, field_validator
from typing import Optional
from datetime import datetime, timezone
import logging
import sys
import math
import os

# =========================
# LOGGER
# =========================

def setup_logger():
    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(handler)

    return logger

logger = setup_logger()

# =========================
# NORMALIZATION & PARSING
# =========================

def normalize_str(v):
    if v is None:
        return None

    v = str(v).strip().lower()

    if v in ("", "nan"):
        return None

    return v

def parse_int(v):
    if v in (None, "", "nan"):
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    try:
        return int(v)
    except:
        raise ValueError(f"int inválido: {v}")

def parse_float(v):
    if v in (None, "", "nan"):
        return None
    try:
        return float(v)
    except:
        raise ValueError(f"float inválido: {v}")

def parse_bool(v):

    if v is None:
        return False

    if isinstance(v, float) and math.isnan(v):
        return False

    if isinstance(v, bool):
        return v

    v = str(v).strip().lower()

    if v in ("", "nan"):
        return False

    if v in ("true", "1"):
        return True

    if v in ("false", "0"):
        return False

    raise ValueError(f"is_comment_enabled inválido: {v}")

def parse_datetime_utc(v):
    if v in (None, "", "nan"):
        return None
    try:
        if str(v).isdigit():
            return datetime.fromtimestamp(int(v), tz=timezone.utc)
        v = str(v).replace(" UTC", "")
        return datetime.fromisoformat(v).replace(tzinfo=timezone.utc)
    except:
        raise ValueError(f"datetime inválido: {v}")

# =========================
# DB CONFIG
# =========================

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "retize_dw",
    "user": "retize",
    "password": "retize_password"
}

DATA_PATH = "files"

FILES = {
    "instagram_comments": "instagram_comments.csv",
    "instagram_media": "instagram_media.csv",
    "instagram_media_insights": "instagram_media_insights.csv",
    "tiktok_comments": "tiktok_comments.csv",
    "tiktok_posts": "tiktok_posts.csv"
}

def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

# =========================
# MODELS
# =========================

class BaseConfig:
    extra = "ignore"

# -------- INSTAGRAM COMMENTS --------
class InstagramComments(BaseModel):
    social_media: str
    post_id: str
    comment_id: str
    comment_timestamp: datetime
    predicted_sentiment: str
    confidence_sentiment: float

    class Config(BaseConfig): pass

    @field_validator("social_media", mode="before")
    def validate_sm(cls, v):
        v = normalize_str(v)
        if v != "instagram":
            raise ValueError("social_media inválido")
        return v

    @field_validator("post_id", "comment_id", mode="before")
    def validate_ids(cls, v):
        v = str(v)
        if not v.isdigit():
            raise ValueError("id inválido")
        return v

    @field_validator("comment_timestamp", mode="before")
    def validate_ts(cls, v):
        return parse_datetime_utc(v)

    @field_validator("confidence_sentiment", mode="before")
    def validate_conf(cls, v):
        v = parse_float(v)
        if v is None or v < 0 or v > 1:
            raise ValueError("confidence inválido")
        return v

    @field_validator("predicted_sentiment", mode="before")
    def validate_sent(cls, v):
        v = normalize_str(v)
        if v not in ["positivo", "neutro", "negativo"]:
            raise ValueError("sentimento inválido")
        return v

# -------- INSTAGRAM MEDIA --------
class InstagramMedia(BaseModel):
    content_source: str
    id: str
    username: str
    timestamp: datetime
    like_count: Optional[int]
    media_type: str
    comments_count: Optional[int]
    is_comment_enabled: bool
    media_product_type: str

    class Config(BaseConfig): pass

    @field_validator("content_source", mode="before")
    def validate_cs(cls, v):
        v = normalize_str(v)
        if v not in ["media", "story"]:
            raise ValueError("content_source inválido")
        return v

    @field_validator("id", mode="before")
    def validate_id(cls, v):
        v = str(v)
        if not v.isdigit():
            raise ValueError("id inválido")
        return v

    @field_validator("username", mode="before")
    def normalize_user(cls, v):
        v = normalize_str(v)
        if v is None:
            raise ValueError("username não pode ser nulo")
        return v

    @field_validator("timestamp", mode="before")
    def validate_ts(cls, v):
        return parse_datetime_utc(v)

    @field_validator("like_count", "comments_count", mode="before")
    def validate_ints(cls, v):
        return parse_int(v)

    @field_validator("is_comment_enabled", mode="before")
    def validate_bool(cls, v):
        try:
            result = parse_bool(v)

            if not isinstance(result, bool):
                raise ValueError(f"is_comment_enabled não retornou bool → {result}")
            return result
        except Exception as e:
            raise ValueError(f"is_comment_enabled inválido → {repr(v)}") from e


    @field_validator("media_type", mode="before")
    def validate_media_type(cls, v):
        v = normalize_str(v)
        if v not in ["carousel_album", "image","video"]:
            raise ValueError("media_type inválido")
        return v
    
    @field_validator("media_product_type", mode="before")
    def validate_media_product_type(cls, v):
        v = normalize_str(v)
        if v not in ["reels", "story","feed"]:
            raise ValueError("media_product_type inválido")
        return v

# -------- INSTAGRAM INSIGHTS --------
class InstagramMediaInsights(BaseModel):
    content_source: str
    id: str
    likes: Optional[int]
    reach: Optional[int]
    saved: Optional[int]
    views: Optional[int]
    shares: Optional[int]
    follows: Optional[int]
    comments: Optional[int]
    replies: Optional[int]
    profile_visits: Optional[int]
    total_interactions: Optional[int]

    class Config(BaseConfig): pass

    @field_validator("content_source", mode="before")
    def validate_cs(cls, v):
        v = normalize_str(v)
        if v not in ["media", "story"]:
            raise ValueError("content_source inválido")
        return v

    @field_validator("id", mode="before")
    def validate_id(cls, v):
        v = str(v)
        if not v.isdigit():
            raise ValueError("id inválido")
        return v

    @field_validator(
        "likes", "reach", "saved", "views", "shares",
        "follows", "comments", "replies",
        "profile_visits", "total_interactions",
        mode="before"
    )
    def validate_ints(cls, v):
        return parse_int(v)

# -------- TIKTOK COMMENTS --------
class TikTokComments(BaseModel):
    social_media: str
    post_id: str
    comment_id: str
    comment_timestamp: datetime
    predicted_sentiment: str
    confidence_sentiment: float

    class Config(BaseConfig): pass

    @field_validator("social_media", mode="before")
    def validate_sm(cls, v):
        v = normalize_str(v)
        if v != "tiktok":
            raise ValueError("social_media inválido")
        return v

    @field_validator("post_id", "comment_id", mode="before")
    def validate_ids(cls, v):
        v = str(v)
        if not v.isdigit():
            raise ValueError("id inválido")
        return v

    @field_validator("comment_timestamp", mode="before")
    def validate_ts(cls, v):
        return parse_datetime_utc(v)

    @field_validator("confidence_sentiment", mode="before")
    def validate_conf(cls, v):
        v = parse_float(v)
        if v is None or v < 0 or v > 1:
            raise ValueError("confidence inválido")
        return v

    @field_validator("predicted_sentiment", mode="before")
    def validate_sent(cls, v):
        v = normalize_str(v)
        if v not in ["positivo", "neutro", "negativo"]:
            raise ValueError("sentimento inválido")
        return v

# -------- TIKTOK POSTS --------
class TikTokPosts(BaseModel):
    item_id: str
    create_time: datetime
    business_username: str
    likes: Optional[int]
    comments: Optional[int]
    shares: Optional[int]
    video_views: Optional[int]
    reach: Optional[int]
    favorites: Optional[int]
    profile_views: Optional[int]
    new_followers: Optional[int]
    total_time_watched: Optional[int]
    average_time_watched: Optional[float]
    full_video_watched_rate: Optional[float]
    video_duration: Optional[float]
    app_download_clicks: Optional[int]
    lead_submissions: Optional[int]
    phone_number_clicks: Optional[int]
    website_clicks: Optional[int]

    class Config(BaseConfig): pass

    @field_validator("item_id", mode="before")
    def validate_id(cls, v):
        v = str(v)
        if not v.isdigit():
            raise ValueError("item_id inválido")
        return v

    @field_validator("business_username", mode="before")
    def normalize_user(cls, v):
        if v is None:
            raise ValueError("business_username inválido")

        v = str(v).strip()

        if not v or v.isdigit():
            raise ValueError("business_username inválido")

        return normalize_str(v)

    @field_validator("create_time", mode="before")
    def validate_ts(cls, v):
        return parse_datetime_utc(v)
    
    @field_validator(
       "average_time_watched", "full_video_watched_rate", "video_duration",
        mode="before"
    )
    def validate_floats(cls, v):
        return parse_float(v)

    @field_validator(
        "likes", "comments", "shares", "video_views", "reach","favorites",
        "profile_views", "new_followers", "total_time_watched",
        "app_download_clicks", "lead_submissions", "phone_number_clicks", "website_clicks",
        mode="before"
    )
    def validate_ints(cls, v):
        return parse_int(v)

# =========================
# VALIDATE
# =========================

def validate_with_pydantic(df, schema):
    valid_rows = []
    invalid_rows = []

    for i, row in df.iterrows():
        data = row.to_dict()
        try:
            obj = schema(**data)
            valid_rows.append(obj.model_dump())
        except ValidationError as e:
            invalid_rows.append({**data, "_error": str(e), "_row": i})
            if len(invalid_rows) <= 5:
                logger.warning(f"linha {i} inválida:\n{e}\n")

    logger.info(f"validação | válidas={len(valid_rows)} inválidas={len(invalid_rows)}")

    return pd.DataFrame(valid_rows), pd.DataFrame(invalid_rows)

# =========================
# LOAD
# =========================

def ensure_schemas(engine):
    schemas = [
        "social_media_raw",
        "social_media_staging",
        "social_media_analytics"
    ]

    with engine.connect() as conn:
        for schema in schemas:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
        
        conn.commit()

def truncate_table(engine, table, schema):
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {schema}.raw_{table}"))
        conn.commit()        

def load_to_db(df, table):
    engine = get_engine()
    ensure_schemas(engine)

    schema = "social_media_raw"
    full_table_name = f"{schema}.raw_{table}"

    with engine.connect() as conn:

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                LIKE {full_table_name} INCLUDING ALL
            );
        """))
        conn.commit()

    try:

        truncate_table(engine, table, schema)
    except Exception:
        pass

    logger.info(f"load {full_table_name} | linhas={len(df)}")

    df.to_sql(
        f"raw_{table}",
        engine,
        schema=schema,
        if_exists="append", 
        index=False
    )
# =========================
# PROCESS
# =========================

def process_file(name, path):
    logger.info(f"processando {name}")

    if not os.path.exists(path):
        logger.warning(f"arquivo não encontrado → {path} | pulando...")
        return
    
    try:
        df = pd.read_csv(path, dtype=str)
    except Exception as e:
        logger.error(f"erro ao ler arquivo {path}: {e}")
        return
    
    df = df.where(pd.notnull(df), None)

    # Remover linhas duplicadas completamente iguais
    df = df.drop_duplicates()

    df_valid, df_invalid = validate_with_pydantic(df, SCHEMA_MAP[name])

    load_to_db(df_valid, name)

    if not df_invalid.empty:
        load_to_db(df_invalid, f"errors_{name}")

    logger.info(f"finalizado {name}\n")

# =========================
# SCHEMA MAP
# =========================

SCHEMA_MAP = {
    "instagram_comments": InstagramComments,
    "instagram_media": InstagramMedia,
    "instagram_media_insights": InstagramMediaInsights,
    "tiktok_comments": TikTokComments,
    "tiktok_posts": TikTokPosts
}

# =========================
# RUN
# =========================

def run():
    logger.info("pipeline iniciado")

    for name, file in FILES.items():
        process_file(name, f"{DATA_PATH}/{file}")

    logger.info("pipeline finalizado")

if __name__ == "__main__":
    run()