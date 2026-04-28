from sqlalchemy import create_engine, text
import pandas as pd

engine = create_engine("postgresql+psycopg2://retize:retize_password@localhost:5432/retize_dw")

# listar tabelas
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT table_schema, table_name
        FROM information_schema.tables
    """))

    print("TABELAS:")
    for row in result:
        print(row)

print("\n====================\n")

# Código para teste de leitura de tabela, ex: stg_tiktok_comments
try:
    df = pd.read_sql("""select
    column_name,
    data_type
from information_schema.columns
where table_schema = 'social_media_staging'
and table_name = 'stg_tiktok_comments';""", engine)
    print("DADOS:")
    print(df.head())
except Exception as e:
    print("ERRO AO LER TABELA:")
    print(e)