import os
import json
from fastapi import FastAPI, Depends, HTTPException, Header
from typing import Optional, List, Dict
from bs4 import BeautifulSoup
import httpx
import redis
from time import sleep

# Constants
API_TOKEN = "static_token"
BASE_URL = "https://dentalstall.com/shop/"
REDIS_HOST = "localhost"
REDIS_PORT = 6379
CACHE_EXPIRY = 3600  # seconds
DATA_FILE = "data/products.json"

# Authentication Middleware
def authenticate(token: str = Header(...)):
    if token != API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

# Scraper Class
class Scraper:
    def __init__(self, base_url: str, proxy: Optional[str] = None, page_limit: int = 5):
        self.base_url = base_url
        self.proxy = proxy
        self.page_limit = page_limit

    async def fetch_page(self, url: str) -> Optional[str]:
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(proxies=self.proxy) as client:
                    response = await client.get(url)
                    response.raise_for_status()
                    return response.text
            except httpx.RequestError as e:
                print(f"Attempt {attempt + 1}: Failed to fetch {url} - {e}")
                sleep(2 ** attempt)
        return None

    async def scrape_products(self) -> List[Dict]:
        products = []
        for page in range(1, self.page_limit + 1):
            url = f"{self.base_url}?page={page}"
            html = await self.fetch_page(url)
            if not html:
                continue
            soup = BeautifulSoup(html, 'html.parser')
            for product in soup.select('.product-card'):
                name = product.select_one('.product-title').text.strip()
                price = float(product.select_one('.product-price').text.strip().replace('$', ''))
                image_url = product.select_one('.product-image')['src']
                products.append({"product_title": name, "product_price": price, "image_url": image_url})
        return products

# Storage Class
class Storage:
    def __init__(self, file_path: str):
        self.file_path = file_path
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

    def save_product(self, product: Dict):
        data = self.load_data()
        data.append(product)
        with open(self.file_path, "w") as f:
            json.dump(data, f, indent=4)

    def load_data(self) -> List[Dict]:
        if os.path.exists(self.file_path):
            with open(self.file_path, "r") as f:
                return json.load(f)
        return []

# Cache Class
class Cache:
    def __init__(self):
        self.client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    def get(self, key: str) -> Optional[Dict]:
        data = self.client.get(key)
        return json.loads(data) if data else None

    def set(self, key: str, value: Dict):
        self.client.setex(key, CACHE_EXPIRY, json.dumps(value))

# Notifier Class
class Notifier:
    def notify(self, message: str):
        print(message)

# FastAPI App
app = FastAPI()

@app.post("/scrape", dependencies=[Depends(authenticate)])
async def scrape_data(page_limit: int = 5, proxy: Optional[str] = None):
    scraper = Scraper(base_url=BASE_URL, proxy=proxy, page_limit=page_limit)
    storage = Storage(DATA_FILE)
    notifier = Notifier()
    cache = Cache()

    products = await scraper.scrape_products()
    new_count = 0

    for product in products:
        cached_product = cache.get(product['product_title'])
        if not cached_product or cached_product['product_price'] != product['product_price']:
            storage.save_product(product)
            cache.set(product['product_title'], product)
            new_count += 1

    notifier.notify(f"{new_count} products scraped and updated.")
    return {"status": "success", "updated_count": new_count}
