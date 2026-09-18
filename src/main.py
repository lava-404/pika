import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from urllib.parse import urljoin

import aio_pika
import httpx
import pymupdf
import serpapi
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from fastapi import FastAPI, File, UploadFile
from langchain_text_splitters import RecursiveCharacterTextSplitter
from pydantic import BaseModel, HttpUrl, TypeAdapter
from sentence_transformers import SentenceTransformer
from supabase import create_client


load_dotenv()


serpapi_client = serpapi.Client(
    api_key=os.getenv("SERPAPI_KEY")
)

supabase = create_client(
    os.getenv("DATABASE_URL"),
    os.getenv("DATABASE_KEY")
)


class Url(BaseModel):
    url: HttpUrl


class SearchRequest(BaseModel):
    query: str


async def search_web(query: str):
    results = serpapi_client.search({
        "engine": "google",
        "q": query,
        "num": 10
    })

    return results


@asynccontextmanager
async def lifespan(app: FastAPI):

    app.state.connection = await aio_pika.connect_robust(
        "amqp://localhost/"
    )

    app.state.channel = await app.state.connection.channel()

    # create/declare the queue
    await app.state.channel.declare_queue("frontier_queue")

    try:
        yield

    finally:
        await app.state.connection.close()


app = FastAPI(
    title="Pika: Web Crawler",
    lifespan=lifespan
)


@app.get("/")
def health():
    return {"status": "ok"}


@app.post("/crawl")
async def push_link(url: Url) -> dict:

    # push the link into the queue
    await publish_url(url)

    return {"status": "queued"}


async def publish_url(url: Url) -> dict:

    await app.state.channel.default_exchange.publish(
        aio_pika.Message(
            body=str(url.url).encode()
        ),
        routing_key="frontier_queue"
    )


@app.post("/search")
async def search(request: SearchRequest):

    results = await search_web(request.query)

    queued = []

    for result in results.get("organic_results", []):

        link = result.get("link")

        if not link:
            continue

        url = Url(
            url=link
        )

        await publish_url(url)

        queued.append(link)

    return {
        "query": request.query,
        "queued": queued
    }
