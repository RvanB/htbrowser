FROM python:3.12-slim AS builder

WORKDIR /app

COPY requirements.txt ./

RUN python -m pip install --no-cache-dir -r requirements.txt

COPY main.py .

RUN python main.py


FROM python:3.12-slim

WORKDIR /app

COPY index.html script.js styles.css ./
COPY --from=builder /app/collection.parquet ./collection.parquet

EXPOSE 8000

CMD ["python", "-m", "http.server", "8000", "--bind", "0.0.0.0"]
