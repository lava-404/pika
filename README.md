Pika

«A distributed web crawler built with Python, FastAPI, RabbitMQ, and Supabase.»

Pika can take either a URL or a search query, discover pages, crawl them asynchronously, and store their extracted content.

How it works

Search / URL
     ↓
  FastAPI
     ↓
   SerpAPI
     ↓
  RabbitMQ
     ↓
 Async Crawler
     ↓
robots.txt → fetch → parse → extract
     ↓
   Supabase

Features

- REST API for crawling and search
- Search-based URL discovery using SerpAPI
- Async crawling with "httpx"
- RabbitMQ-based crawl queue
- "robots.txt" support
- URL deduplication
- HTML parsing with BeautifulSoup
- Automatic link discovery
- Relative → absolute URL resolution
- HTTP/HTTPS filtering
- Persistent storage in Supabase

API

Crawl a URL

curl -X POST http://127.0.0.1:8000/crawl \
  -H "Content-Type: application/json" \
  -d '{"url":"https://example.com"}'

Search and crawl

curl -X POST http://127.0.0.1:8000/search \
  -H "Content-Type: application/json" \
  -d '{"query":"distributed systems internships India"}'

Search results are added to the RabbitMQ frontier and processed by the crawler.

Storage

Crawled pages are stored in Supabase:

CREATE TABLE url_metadata (
    url TEXT PRIMARY KEY,
    page_text TEXT
);

Run

Start RabbitMQ, then:

uv run uvicorn src.main:app --reload

and in another terminal:

uv run python src/worker.py

Stack

Python · FastAPI · RabbitMQ · aio-pika · httpx · BeautifulSoup · SerpAPI · Supabase · uv
