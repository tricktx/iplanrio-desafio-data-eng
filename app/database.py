import duckdb
from src.utils.setup import get_gcs_token

def get_connection():
    
    con = duckdb.connect()

    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    print(get_gcs_token)

    con.execute("DROP SECRET IF EXISTS gcs_secret;")
    con.execute(f"""
        CREATE SECRET (
            TYPE GCS,
            BEARER_TOKEN '{get_gcs_token()}'
        );
    """)

    con.execute("""
        ATTACH 'gs://br-cgu-terceirizados/gold/terceirizados-gold.duckdb'
        AS gold_db (READ_ONLY);
    """)
    
    return con