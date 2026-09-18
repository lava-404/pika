from fastapi import FastAPI, UploadFile, File
import pymupdf
import uuid
import aio_pika
from pydantic import BaseModel, HttpUrl, TypeAdapter
import asyncio
from urllib.parse import urljoin
import os
import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from langchain_text_splitters import RecursiveCharacterTextSplitter
from supabase import create_client
from sentence_transformers import SentenceTransformer
from contextlib import asynccontextmanager


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

app = FastAPI(title="Pika: Web Crawler", lifespan=lifespan)

class Url(BaseModel):
    url: HttpUrl


load_dotenv()

supabase = create_client(
    os.getenv("DATABASE_URL"),
    os.getenv("DATABASE_KEY")
)


@app.get("/")
def health():
    return {"status": "ok"}


@app.post("/crawl")
async def push_link(url: Url) -> dict:
    #push the link into the queue
    await publish_url(url)
    return {"status": "queued"}

async def publish_url(url: Url) -> dict:
    await app.state.channel.default_exchange.publish(
        aio_pika.Message(body=str(url.url).encode()),
        routing_key="frontier_queue"
    )
    