import pandahouse as ph

connection = {'host': 'http://localhost:8123/',
              'database': 'airflow',
              'user': 'airflow',
              'password': 'airflow'}


query = 'SELECT * FROM Books'
df = ph.read_clickhouse(query, connection=connection)

print(df.head())