FROM python:3.10 as builder

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl libpq-dev git\
    && rm -rf /var/lib/apt/lists/* /usr/share/doc /usr/share/man \
    && apt-get clean


COPY requirements.txt .
COPY requirements-no-deps.txt .

RUN pip wheel --no-cache-dir --no-deps --wheel-dir /app/wheels -r requirements.txt
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /app/wheels-no -r requirements-no-deps.txt


FROM python:3.10

WORKDIR /app

RUN useradd --create-home python \
    && chown python:python -R /app

USER python

COPY --from=builder --chown=python:python /app/wheels-no /wheels-no
COPY --from=builder --chown=python:python /app/wheels /wheels

RUN pip install --no-cache --user /wheels-no/*
RUN pip install --no-cache --user /wheels/*

ENV PYTHONUNBUFFERED="true" \
    PYTHONPATH="." \
    PATH="${PATH}:/home/python/.local/bin" \
    USER="python"

COPY --chown=python:python . .

EXPOSE 8000

CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "-c", "python:config.gunicorn", "app.main:app"]