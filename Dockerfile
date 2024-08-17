FROM python:3.12 AS builder

RUN pip install poetry==1.8.3

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_VIRTUALENVS_OPTIONS_NO_PIP=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /app

COPY pyproject.toml poetry.lock ./
RUN touch README.md

RUN --mount=type=cache,target=$POETRY_CACHE_DIR poetry install --no-root

FROM python:3.12-slim AS runtime

ENV VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

RUN groupadd --gid 1000 nonroot \
    && useradd --uid 1000 --gid 1000 --no-create-home --shell /bin/bash nonroot \

COPY --from=builder --chown=nonroot:nonroot ${VIRTUAL_ENV} ${VIRTUAL_ENV}
COPY --link --chown=nonroot:nonroot . /app

USER nonroot
WORKDIR /app
ENTRYPOINT ["scrapy"]
