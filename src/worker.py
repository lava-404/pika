
import asyncio
import os
from urllib.parse import urljoin, urlparse

import aio_pika
import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pydantic import BaseModel, HttpUrl
from supabase import create_client


load_dotenv()

supabase = create_client(
    os.getenv("DATABASE_URL"),
    os.getenv("DATABASE_KEY")
)


class Url(BaseModel):
    url: HttpUrl


async def publish_url(url: Url):
    connection = await aio_pika.connect_robust("amqp://localhost/")
    channel = await connection.channel()

    await channel.declare_queue("frontier_queue")

    await channel.default_exchange.publish(
        aio_pika.Message(
            body=str(url.url).encode()
        ),
        routing_key="frontier_queue"
    )

    await connection.close()


async def worker():
    connection = await aio_pika.connect_robust("amqp://localhost/")
    channel = await connection.channel()
    queue = await channel.declare_queue("frontier_queue")

    async with httpx.AsyncClient() as client:
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():

                    url = message.body.decode()

                    # Check if URL was already crawled
                    result = (
                        supabase
                        .table("url_metadata")
                        .select("url")
                        .eq("url", url)
                        .execute()
                    )

                    if result.data:
                        print("Already crawled, skipping:", url)
                        continue

                    print("Crawling:", url)

                    response = await client.get(url)

                    soup = BeautifulSoup(response.text, "lxml")

                    page_text = soup.get_text(" ", strip=True)

                    # Store crawled page
                    supabase.table("url_metadata").insert({
                        "url": url,
                        "page_text": page_text
                    }).execute()

                    # Find new URLs
                    for link in soup.find_all("a", href=True):

                        absolute_url = urljoin(url, link["href"])

                        scheme = urlparse(absolute_url).scheme

                        if scheme not in ("http", "https"):
                            continue

                        new_url = Url(url=absolute_url)

                        await publish_url(new_url)

if __name__ == "__main__":
    asyncio.run(worker())

