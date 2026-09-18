
import asyncio
import os
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
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

robots_cache = {}

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

                    if not await can_crawl(url, client):
                        print("Blocked by robots.txt:", url)
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


async def can_crawl(url: str, client: httpx.AsyncClient) -> bool:
    parsed = urlparse(url)

    base_url = f"{parsed.scheme}://{parsed.netloc}"
    robots_url = f"{base_url}/robots.txt"

    if base_url not in robots_cache:
        try:
            response = await client.get(robots_url)

            if response.status_code >= 400:
                # No accessible robots.txt
                robots_cache[base_url] = None
            else:
                parser = RobotFileParser()
                parser.set_url(robots_url)
                parser.parse(response.text.splitlines())

                robots_cache[base_url] = parser

        except httpx.RequestError:
            # If we can't retrieve robots.txt, be conservative
            return False

    parser = robots_cache[base_url]

    if parser is None:
        return True

    return parser.can_fetch("*", url)

if __name__ == "__main__":
    asyncio.run(worker())

