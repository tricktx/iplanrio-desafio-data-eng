from app.database import get_connection

def get_terceirizados(offset: int = 0, limit: int = 10):
    conn = get_connection()
    query = f"""
        SELECT 
            id,
            sigla_orgao_superior,
            cnpj_empresa,
            cpf
        FROM br_cgu_terceirizados.gold
        LIMIT {limit} OFFSET {offset}
    """
    return conn.execute(query).fetchdf().to_dict(orient='records')


def get_terceirizado_by_id(id: int):
    conn = get_connection()
    query = f"""
        SELECT *
        FROM br_cgu_terceirizados.gold
        WHERE id = {id}
    """
    return conn.execute(query).fetchdf().to_dict(orient='records')