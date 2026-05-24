FROM apache/airflow:2.11.0

USER root
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY pyproject.toml .
RUN uv pip install --system --no-cache -r pyproject.toml

USER airflow
