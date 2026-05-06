import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

def get_engine():
    host     = os.getenv("WEATHER_DB_HOST")
    port     = os.getenv("WEATHER_DB_PORT", "5432")
    db       = os.getenv("WEATHER_DB_NAME")
    user     = os.getenv("WEATHER_DB_USER")
    password = os.getenv("WEATHER_DB_PASSWORD")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)

def load_raw(df: pd.DataFrame) -> int:
    if df.empty:
        logger.warning("Empty DataFrame — skipping load")
        return 0
    engine = get_engine()

    if "local_time" in df.columns:
        df["local_time"] = pd.to_datetime(df["local_time"], errors="coerce")
        
    df.to_sql(
        name="weather_raw", schema="raw",
        con=engine, if_exists="append",
        index=False, method="multi"
    )
    logger.info(f"Loaded {len(df)} rows into raw.weather_raw")
    engine.dispose()
    return len(df)

def load_summary():
    engine = get_engine()
    sql = """
        INSERT INTO analytics.weather_summary (
            city, country, record_date,
            avg_temperature_c, max_temperature_c, min_temperature_c,
            avg_humidity, avg_wind_speed_kmh,
            dominant_condition, total_records, last_updated
        )
        SELECT
            city, country,
            DATE(ingested_at)          AS record_date,
            ROUND(AVG(temperature_c)::NUMERIC, 2),
            ROUND(MAX(temperature_c)::NUMERIC, 2),
            ROUND(MIN(temperature_c)::NUMERIC, 2),
            ROUND(AVG(humidity)::NUMERIC, 2),
            ROUND(AVG(wind_speed_kmh)::NUMERIC, 2),
            MODE() WITHIN GROUP (ORDER BY weather_description),
            COUNT(*),
            NOW()
        FROM raw.weather_raw
        GROUP BY city, country, DATE(ingested_at)
        ON CONFLICT (city, record_date)
        DO UPDATE SET
            avg_temperature_c  = EXCLUDED.avg_temperature_c,
            max_temperature_c  = EXCLUDED.max_temperature_c,
            min_temperature_c  = EXCLUDED.min_temperature_c,
            avg_humidity       = EXCLUDED.avg_humidity,
            avg_wind_speed_kmh = EXCLUDED.avg_wind_speed_kmh,
            dominant_condition = EXCLUDED.dominant_condition,
            total_records      = EXCLUDED.total_records,
            last_updated       = NOW();
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    logger.info("analytics.weather_summary refreshed")
    engine.dispose()