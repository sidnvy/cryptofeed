FROM python:3.10-slim-bullseye as base
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100
WORKDIR /app

FROM base as builder

RUN apt-get update && apt-get install -y gcc git g++

RUN pip install build

COPY cryptofeed/ ./cryptofeed
COPY setup.py pyproject.toml *.md .
RUN python -m build --wheel

RUN pip install ./dist/*.whl --force
RUN pip install aioredis
RUN pip install hiredis
RUN pip install redis
RUN pip install pymongo[srv]
RUN pip install motor
RUN pip install asyncpg
RUN pip install aiokafka
RUN pip install s3fs
RUN pip install pyarrow
RUN pip install pandas


FROM base as application
COPY --from=builder /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
COPY cryptostore.py /cryptostore.py

CMD ["/cryptostore.py"]
ENTRYPOINT ["python"]
