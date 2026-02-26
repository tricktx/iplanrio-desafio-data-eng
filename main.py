# import duckdb
# import subprocess
# from dateutil.relativedelta import relativedelta

# # https://medium.com/@404c3s4r/subprocess-no-python-937a3c3bd518
# result = subprocess.run(['gcloud', 'auth', 'application-default', 'print-access-token'], 
#                         capture_output=True, text=True)

# access_token = result.stdout.strip()

# con = duckdb.connect()
# con.execute("INSTALL httpfs")
# con.execute("LOAD httpfs")
# con.execute("DROP SECRET IF EXISTS gcs_secret")
# con.execute(f"CREATE SECRET gcs_secret (TYPE GCS, bearer_token '{access_token}')")

# result = con.execute("""
#                       SELECT MAX(
#                           make_date(
#                               CAST(Ano_Carga AS INT),
#                               CAST(Num_Mes_Carga AS INT),
#                               1
#                           )
#                       ) as date
#                       FROM read_parquet('gs://br-cgu-terceirizados/terceirizados/*.parquet') limit 10
#                       """).fetchone()[0]

# print(f"Max date in DuckDB: {str(result)}")
# print(f"Max date after adding 4 months: {str(result + relativedelta(months=4))}")

# result_after_four_month = str(result + relativedelta(months=4))[0:7].replace('-', '')

# breakpoint()

import duckdb

con = duckdb.connect('duckdb/terceirizados-silver.duckdb')


df = con.execute("""
  SELECT * FROM br_cgu_terceirizados.silver
  
""").df()

breakpoint()