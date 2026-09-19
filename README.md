# Pika

> A distributed web crawler built with Python, FastAPI, RabbitMQ, and Supabase.

Pika accepts a **URL or search query**, discovers pages, crawls them asynchronously, extracts their content and links, and stores the results.

## Architecture

```text
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
