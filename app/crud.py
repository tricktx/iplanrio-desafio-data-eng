from app.database import get_connection

def get_terceirizados(offset: int = 0, limit: int = 10):
    """
    Retrieve a paginated list of terceirizados (outsourced workers) from the CGU database.
    
    Fetches records from the br_cgu_terceirizados.gold table containing information about
    outsourced workers, including their associated organizations and companies.
    
    Args:
        offset (int, optional): The number of records to skip from the beginning. 
                                Defaults to 0.
        limit (int, optional): The maximum number of records to return. 
                                Defaults to 10.
    """
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
    """
    Retrieve a terceirizado (outsourced service provider) record by ID.

    This function queries the 'br_cgu_terceirizados.gold' table to fetch a single
    terceirizado record matching the provided ID.

    Args:
        id (int): The unique identifier of the terceirizado record to retrieve.
        
    """
    conn = get_connection()
    query = f"""
        SELECT *
        FROM br_cgu_terceirizados.gold
        WHERE id = {id}
    """
    return conn.execute(query).fetchdf().to_dict(orient='records')
            