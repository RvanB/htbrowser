# HathiFile Viewer

A interface for exploring the public domain items in HathiTrust without requiring search. Backed by a parquet-based database generated from the hathifiles.

![HathiFile Viewer screenshot](img/screenshot.png)

## Run locally with pip

Install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Generate the parquet file:

```bash
python main.py
```

Then serve the current directory over HTTP:

```bash
python -m http.server 8000
```

Open <http://localhost:8000>.

## Run with Docker Compose

Build the image, generate `collection.parquet` inside the container image, and start the HTTP server:

```bash
docker compose up --build
```

Then open <http://localhost:8000>.
