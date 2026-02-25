import duckdb
import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)
# # Get access token
# result = subprocess.run(['gcloud', 'auth', 'application-default', 'print-access-token'], 
#                         capture_output=True, text=True)
# access_token = result.stdout.strip()

# con = duckdb.connect()
# con.execute("INSTALL httpfs")
# con.execute("LOAD httpfs")
# con.execute("DROP SECRET IF EXISTS gcs_secret")
# con.execute(f"CREATE SECRET gcs_secret (TYPE GCS, bearer_token '{access_token}')")

# # This works perfectly
# result = con.execute("SELECT * FROM read_parquet('gs://br-cgu-terceirizados/data.parquet') limit 10").fetchone()
# breakpoint()
# print(result)
# print(f"Success: {result[0]} rows")

# con = duckdb.connect("terceirizados-silver.duckdb")

# result = con.execute("SELECT * FROM br_cgu_terceirizados.silver").df()
# breakpoint()
# print(result)

# con.close()