FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

COPY pyproject.toml uv.lock* README.md ./ 

ENV UV_SYSTEM_PYTHON=1

COPY . .

RUN uv sync && uv run dbt deps


ENTRYPOINT ["uv", "run", "--"]