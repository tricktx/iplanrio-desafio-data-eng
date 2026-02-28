from fastapi import FastAPI, HTTPException, Query
from functools import lru_cache
from app.crud import get_terceirizados, get_terceirizado_by_id

app = FastAPI(title="API Terceirizados")

@app.get("/")
def root():
    return {"status": "API Terceirizados ativa"}

@lru_cache(maxsize=1000)
def cached_pages_terceirizados(page: int, limit: int):
    return get_terceirizados(page * limit, limit)

@app.get("/terceirizados/pages/{page}")
def pages_terceirizados(
    page: int,
    limit: int = Query(100, ge=1, le=100)
):
    result = cached_pages_terceirizados(page, limit)
    if not result:
        raise HTTPException(status_code=404, detail="Não encontrado")
    return result
    


@lru_cache(maxsize=1000)
def cached_id_terceirizado(id: int):
    return get_terceirizado_by_id(id)


@app.get("/terceirizados/{id}")
def id_terceirizado(id: int):
    result = cached_id_terceirizado(id)
    if not result:
        raise HTTPException(status_code=404, detail="Não encontrado")
    return result