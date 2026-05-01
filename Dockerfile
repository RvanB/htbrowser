FROM python:3.12-slim AS builder

WORKDIR /app

RUN python -m pip install --no-cache-dir \
    "duckdb>=1.5.2" \
    "pandas>=3.0.2" \
    "pyarrow>=24.0.0" \
    "python-dateutil>=2.9.0.post0" \
    "requests>=2.33.1" \
    "tqdm>=4.67.3"

COPY main.py .

RUN python main.py


FROM python:3.12-slim

WORKDIR /app

COPY index.html script.js styles.css ./
COPY --from=builder /app/collection.parquet ./collection.parquet

EXPOSE 8000

CMD ["python", "-m", "http.server", "8000", "--bind", "0.0.0.0"]
