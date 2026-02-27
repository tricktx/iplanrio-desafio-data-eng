import duckdb

def get_connection():
    return duckdb.connect('duckdb/terceirizados-gold.duckdb', read_only=True)