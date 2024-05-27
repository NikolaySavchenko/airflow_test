from clickhouse_sqlalchemy import make_session, get_declarative_base, types, engines
from sqlalchemy import create_engine, text, MetaData, Column

# Создаем подключение к Clickhouse
engine = create_engine('clickhouse+native://localhost:9000/airflow')

# Создаем сессию
session = make_session(engine)

# Создаем новую базу данных
session.execute(text('CREATE DATABASE IF NOT EXISTS airflow'))

engine = create_engine('clickhouse+native://airflow:airflow@localhost:9000/airflow')

session = make_session(engine)

metadata = MetaData(bind=engine)

Base = get_declarative_base(metadata=metadata)

class Rate(Base):
    title = Column(types.text, primary_key=True)
    author = Column(types.text)

    __table_args__ = (
        engines.Memory(),
    )

# Emits CREATE TABLE statement
Rate.__table__.create()

# Проверяем подключение
# result = session.execute('SELECT 1')
# print(result.fetchall())

# create_table_query = '''
# CREATE TABLE IF NOT EXISTS books_table (
#   id UInt64,
#   author String
#   title String
# ) ENGINE = MergeTree()
# ORDER BY id
# '''
# engine.execute(create_table_query)
#
# data = {'id': [1, 2, 3], 'author': ['John', 'Jane', 'Bob'], 'title': ['81 for', 'Black Swan', 'Potter']}
# df = pd.DataFrame(data)
#
# table_name = 'books'
# df.to_sql(table_name, engine, if_exists='append', index=False)
