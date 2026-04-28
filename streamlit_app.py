import streamlit as st
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'retize_dw',
    'user': 'retize',
    'password': 'retize_password',
}

MART_TABLES = [
    'posts',
    'comentários',
]


def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )


@st.cache_data
def load_data(sql):
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)


@st.cache_data
def list_sql_files():
    sql_dir = Path(__file__).resolve().parent / 'sql'
    files = list(sql_dir.glob('*.sql'))
    def sort_key(f):
        import re
        match = re.search(r'(\d+)', f.name)
        return int(match.group(1)) if match else 0
    return sorted(files, key=sort_key)


@st.cache_data
def get_distinct_values(table: str, column: str):
    sql = f"SELECT DISTINCT {column} FROM social_media_analytics.{table} WHERE {column} IS NOT NULL ORDER BY {column} LIMIT 500"
    df = load_data(sql)
    if column not in df.columns:
        return []
    return sorted(df[column].astype(str).unique().tolist())


def read_sql_file(path: Path) -> str:
    text = path.read_text(encoding='utf-8')
    return text


def extract_description(sql_text: str) -> str:
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith('#'):
            return stripped.lstrip('# ').strip()
        if stripped:
            break
    return 'Consulta de negócio'


def extract_query(sql_text: str) -> str:
    lines = [line for line in sql_text.splitlines() if not line.strip().startswith('#')]
    query = '\n'.join(lines).strip()
    return query


def format_float(value, decimals=2):
    if value is None:
        return '0.00'
    try:
        return f"{float(value):.{decimals}f}"
    except Exception:
        return str(value)


def build_filter_clause(filters: dict) -> str:
    clauses = []
    for column, values in filters.items():
        if not values:
            continue
        if len(values) == 1:
            escaped_value = values[0].replace("'", "''")
            clauses.append(f"{column} = '{escaped_value}'")
        else:
            escaped = [v.replace("'", "''") for v in values]
            values_list = ', '.join(f"'{v}'" for v in escaped)
            clauses.append(f"{column} IN ({values_list})")
    return ' AND '.join(clauses)


def render_metrics(table: str, where_clause: str):
    table_map = {'posts': 'mart_posts_enriched', 'comentários': 'mart_comments_enriched'}
    real_table = table_map.get(table, table)
    if table == 'posts':
        sql = f"SELECT COUNT(*) AS total_posts, AVG(engagement) AS avg_engagement, AVG(reach) AS avg_reach, SUM(likes) AS total_likes, SUM(comments) AS total_comments, SUM(shares) AS total_shares, SUM(saves) AS total_saves FROM social_media_analytics.{real_table} {where_clause};"
    else:
        sql = f"SELECT COUNT(*) AS total_comments, AVG(confidence_sentiment) AS avg_confidence, AVG(sentiment_value) AS avg_sentiment_value, SUM(CASE WHEN predicted_sentiment = 'positivo' THEN 1 ELSE 0 END) AS positive_comments, SUM(CASE WHEN predicted_sentiment = 'neutro' THEN 1 ELSE 0 END) AS neutral_comments, SUM(CASE WHEN predicted_sentiment = 'negativo' THEN 1 ELSE 0 END) AS negative_comments FROM social_media_analytics.{real_table} {where_clause};"

    return load_data(sql)


def render_dashboard():
    st.title('Retize - Engajamento em redes sociais')
    st.markdown('Visualize métricas e amostras filtradas para cada tabela de modelo.')

    table = st.sidebar.selectbox('Tabelas mart', MART_TABLES)
    filters = {}

    table_map = {'posts': 'mart_posts_enriched', 'comentários': 'mart_comments_enriched'}
    real_table = table_map[table]

    platform_values = get_distinct_values(real_table, 'platform')
    if platform_values:
        selected_platform = st.sidebar.selectbox('Platform', ['Todas'] + platform_values)
        if selected_platform != 'Todas':
            filters['platform'] = [selected_platform]

    username_values = get_distinct_values(real_table, 'username')
    if username_values:
        selected_username = st.sidebar.selectbox('Username', ['Todas'] + username_values)
        if selected_username != 'Todas':
            filters['username'] = [selected_username]

    if table == 'posts':
        media_types = get_distinct_values(real_table, 'media_type')
        if media_types:
            selected_media_type = st.sidebar.selectbox('Media type', ['Todas'] + media_types)
            if selected_media_type != 'Todas':
                filters['media_type'] = [selected_media_type]
    else:
        sentiments = get_distinct_values(real_table, 'predicted_sentiment')
        if sentiments:
            selected_sentiment = st.sidebar.selectbox('Sentimento', ['Todas'] + sentiments)
            if selected_sentiment != 'Todas':
                filters['predicted_sentiment'] = [selected_sentiment]

    where_clause = ''
    if filters:
        clause = build_filter_clause(filters)
        where_clause = f'WHERE {clause}'

    try:
        metrics = render_metrics(table, where_clause)
        if not metrics.empty:
            if table == 'posts':
                col1, col2, col3 = st.columns(3)
                col1.metric('Total de posts', int(metrics['total_posts'].iloc[0]))
                col2.metric('Engajamento médio', format_float(metrics['avg_engagement'].iloc[0]))
                col3.metric('Alcance médio', format_float(metrics['avg_reach'].iloc[0]))
                col4, col5, col6 = st.columns(3)
                col4.metric('Curtidas totais', int(metrics['total_likes'].iloc[0] or 0))
                col5.metric('Comentários totais', int(metrics['total_comments'].iloc[0] or 0))
                col6.metric('Compartilhamentos totais', int(metrics['total_shares'].iloc[0] or 0))
                st.metric('Salvamentos totais', int(metrics['total_saves'].iloc[0] or 0))
            else:
                col1, col2, col3 = st.columns(3)
                col1.metric('Total de comentários', int(metrics['total_comments'].iloc[0]))
                col2.metric('Confiança média', format_float(metrics['avg_confidence'].iloc[0]))
                col3.metric('Valor médio de sentimento', format_float(metrics['avg_sentiment_value'].iloc[0]))
                st.write(
                    f"Positivos: {int(metrics['positive_comments'].iloc[0] or 0)} | "
                    f"Neutros: {int(metrics['neutral_comments'].iloc[0] or 0)} | "
                    f"Negativos: {int(metrics['negative_comments'].iloc[0] or 0)}"
                )

        query = f'SELECT * FROM social_media_analytics.{real_table} {where_clause} LIMIT 100;'
        df = load_data(query)
        df = df.round(2)
        st.markdown('---')
        st.subheader('Amostra filtrada')
        st.dataframe(df, use_container_width=True)
    except SQLAlchemyError as err:
        st.error(f'Erro ao consultar o banco de dados: {err}')
        st.info('Execute o pipeline de ingestão e transformação primeiro.')


def render_business_queries():
    st.title('Perguntas de negócio')
    st.markdown('Cada página abaixo reflete um arquivo SQL em `sql/` com a resposta a uma pergunta de negócio.')

    files = list_sql_files()
    if not files:
        st.warning('Nenhum arquivo SQL encontrado na pasta `sql/`.')
        return

    choices = {file.stem: file for file in files}
    selected = st.sidebar.selectbox('Consulta', list(choices.keys()), index=0)
    sql_file = choices[selected]

    sql_text = read_sql_file(sql_file)
    description = extract_description(sql_text)
    query = extract_query(sql_text)

    st.header(description)
    with st.expander('SQL da consulta', expanded=False):
        st.code(query, language='sql')

    try:
        data = load_data(query)
        st.markdown(f'**Tabela de resultados:** {len(data)} registros')
        st.dataframe(data)
    except SQLAlchemyError as err:
        st.error(f'Erro ao executar a consulta: {err}')
        st.info('Verifique se o pipeline já foi executado e se os modelos dbt existem no schema correto.')


st.set_page_config(page_title='Retize Analytics', layout='wide')

st.sidebar.title('Navegação')
section = st.sidebar.radio('Seção', ['Tabela Análitica', 'Query das perguntas de negócio'])

if section == 'Tabela Análitica':
    render_dashboard()
else:
    render_business_queries()
