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
    
    Returns:
        list[dict]: A list of dictionaries, each containing:
            - id (int): Unique identifier for the terceirizado record
            - sigla_orgao_superior (str): Acronym of the superior government organization
            - cnpj_empresa (str): CNPJ (Brazilian company tax ID) of the company
            - cpf (str): CPF (Brazilian individual tax ID) of the terceirizado worker
    
    Example:
        >>> records = get_terceirizados(offset=0, limit=5)
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

    Returns:
        list[dict]: A list containing a single dictionary with the terceirizado record data.
                   Each dictionary contains all columns from the gold table.
                   Returns an empty list if no record matches the given ID.

    Raises:
        Exception: May raise database connection or query execution errors.

    Example:
        >>> result = get_terceirizado_by_id(123)
        >>> # Returns: [{'id': 123, 'sigla_orgao_superior': '...', 'cnpj_empresa': '...'}]
    """"""
    conn = get_connection()
    query = f"""
        SELECT *
        FROM br_cgu_terceirizados.gold
        WHERE id = {id}
    """
    return conn.execute(query).fetchdf().to_dict(orient='records')