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


app = FastAPI()

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

app = FastAPI(lifespan=lifespan)

class Url(BaseModel):
    url: HttpUrl

app = FastAPI(title="Pika: Web Crawler")

load_dotenv()

supabase = create_client(
    os.getenv("DATABASE_URL"),
    os.getenv("DATABASE_KEY")
)

async def worker():
    connection = await aio_pika.connect_robust("amqp://localhost/")
    channel = await connection.channel()
    queue = await channel.declare_queue("frontier_queue")

    async with httpx.AsyncClient() as client:
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    url = message.body.decode()

                    response = await client.get(url)
                    soup = BeautifulSoup(response.text, "lxml")

                    for link in soup.find_all("a", href=True):
                        absolute_url = urljoin(url, link["href"])
                        new_url = Url(url=absolute_url)
                        await push_link(new_url)
asyncio.run(worker())

@app.get("/")
def health():
    return {"status": "ok"}


@app.post("/crawl/{url_link}")
async def push_link(url: Url) -> dict:
    #push the link into the queue
    await app.state.channel.default_exchange.publish(
        aio_pika.Message(body=str(url.url).encode()),
        routing_key="frontier_queue"
    )
    return {"status": "queued"}


