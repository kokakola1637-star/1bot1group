import asyncio
import logging
import random
import os
import concurrent.futures
from urllib.parse import urljoin, quote

import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from bs4 import BeautifulSoup

from fastapi import FastAPI, Request
from fastapi.responses import Response

# Load your config
import config

# --- LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- DATABASE ASYNC WRAPPER ---
DB_NAME = "bot_database.db"
db_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

def ensure_db_exists_sync():
    import sqlite3
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sent_videos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_url TEXT UNIQUE,
            video_title TEXT,
            category TEXT,
            sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_url ON sent_videos(video_url)')
    conn.commit()
    conn.close()

async def is_duplicate(video_url):
    def _check():
        import sqlite3
        try:
            conn = sqlite3.connect(DB_NAME)
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM sent_videos WHERE video_url = ? LIMIT 1", (video_url,))
            result = cursor.fetchone()
            conn.close()
            return result is not None
        except Exception as e:
            logger.error(f"DB Read Error: {e}")
            return True
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(db_executor, _check)

async def save_video(video_url, title, category):
    def _save():
        import sqlite3
        try:
            conn = sqlite3.connect(DB_NAME)
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR IGNORE INTO sent_videos (video_url, video_title, category) VALUES (?, ?, ?)",
                (video_url, title, category)
            )
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"DB Write Error: {e}")
            return False
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(db_executor, _save)

# --- SCRAPERS ---

class Scraper:
    @staticmethod
    async def get_page(url, session):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        try:
            async with session.get(url, headers=headers, timeout=20) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    logger.warning(f"HTTP {response.status} on {url}")
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
        return None

    @staticmethod
    async def scrape_generic(url, category, session):
        # Generic fallback: tries to find links inside the page
        html = await Scraper.get_page(url, session)
        if not html: return []
        soup = BeautifulSoup(html, 'html.parser')
        results = []
        
        # Attempt to make URL search-friendly
        parsed = list(urlparse(url))
        query = f"?q={quote(category)}" if not parsed[4] else f"&q={quote(category)}"
        # Only modify if it looks like a standard domain, not a specific tag page
        if "tag/" not in url and "cat/" not in url:
            search_url = urljoin(url, query)
            html = await Scraper.get_page(search_url, session)
            if html: soup = BeautifulSoup(html, 'html.parser')

        # Find links that look like video pages
        for a in soup.find_all('a', href=True):
            href = a['href']
            if len(href) > 20: 
                img = a.find('img')
                thumb_url = None
                if img:
                    thumb_url = img.get('src') or img.get('data-src')
                    if thumb_url and not thumb_url.startswith('http'):
                        thumb_url = urljoin(url, thumb_url)
                
                full_link = urljoin(url, href)
                title = a.get_text(strip=True)[:50]
                results.append({'url': full_link, 'thumb': thumb_url, 'title': title})
        return results

    @staticmethod
    async def scrape_desixclip(url, category, session):
        # Desixclip uses ?s=searchterm
        html = await Scraper.get_page(f"{url}?s={quote(category)}", session)
        if not html: return []
        soup = BeautifulSoup(html, 'html.parser')
        results = []
        for item in soup.select('article'): # Standard WP theme
            a_tag = item.find('a', href=True)
            if a_tag:
                href = urljoin(url, a_tag['href'])
                img_tag = item.find('img')
                thumb = img_tag.get('src') if img_tag else None
                if thumb and not thumb.startswith('http'): thumb = urljoin(url, thumb)
                
                results.append({
                    'url': href,
                    'thumb': thumb,
                    'title': a_tag.get_text(strip=True)[:50]
                })
        return results

    @staticmethod
    async def scrape_kamababa(url, category, session):
        html = await Scraper.get_page(url, session)
        if not html: return []
        soup = BeautifulSoup(html, 'html.parser')
        results = []
        # Kamababa usually has links like /video/
        for a in soup.find_all('a', href=True):
            href = a['href']
            if '/video/' in href:
                full_link = urljoin(url, href)
                img = a.find('img')
                thumb = img.get('src') if img else None
                if thumb and not thumb.startswith('http'): thumb = urljoin(url, thumb)
                
                results.append({
                    'url': full_link,
                    'thumb': thumb,
                    'title': a.get_text(strip=True)[:50]
                })
        return results

    @staticmethod
    async def scrape_hindichudaivideos(url, category, session):
        # Specific logic for HindiChudaiVideos
        # URL structure: https://www.hindichudaivideos.com/search/keyword
        search_url = urljoin(url, f"search/{quote(category)}/")
        html = await Scraper.get_page(search_url, session)
        if not html: return []
        soup = BeautifulSoup(html, 'html.parser')
        results = []
        
        # Selectors based on common tube themes
        for item in soup.select('div.video-item, div.item, article, li.video'):
            a_tag = item.find('a', href=True)
            if a_tag:
                href = urljoin(url, a_tag['href'])
                img_tag = item.find('img')
                thumb = img_tag.get('src') if img_tag else None
                if thumb and not thumb.startswith('http'): thumb = urljoin(url, thumb)
                
                title = a_tag.get('title') or a_tag.get_text(strip=True)
                results.append({
                    'url': href,
                    'thumb': thumb,
                    'title': title[:50]
                })
        return results

    @staticmethod
    async def scrape_mydesi2(url, category, session):
        # MyDesi2 often uses pagination /category/keyword
        search_url = urljoin(url, f"search/{quote(category)}/")
        html = await Scraper.get_page(search_url, session)
        if not html: return []
        soup = BeautifulSoup(html, 'html.parser')
        results = []
        
        # Often uses <div class="video-block"> or similar
        for item in soup.find_all('a', href=True):
            href = a['href']
            # Filter to ensure it's a video link, not a tag or category
            if len(href) > 10 and not any(x in href for x in ['/tag/', '/category/', '/page/']):
                 full_link = urljoin(url, href)
                 # Heuristic: check for image sibling
                 img = item.find('img')
                 if img:
                     thumb = img.get('src')
                     if not thumb.startswith('http'): thumb = urljoin(url, thumb)
                     results.append({
                        'url': full_link,
                        'thumb': thumb,
                        'title': img.get('alt')[:50]
                     })
        return results

    @staticmethod
    async def scrape_eporner(url, category, session):
        # Eporner is large, we use their search
        search_url = f"https://www.eporner.com/search/{quote(category)}/"
        html = await Scraper.get_page(search_url, session)
        if not html: return []
        soup = BeautifulSoup(html, 'html.parser')
        results = []
        
        for a in soup.find_all('a', href=True):
            if 'eporner.com/video-' in a['href']:
                href = urljoin(url, a['href'])
                img = a.find('img')
                thumb = img.get('src') if img else None
                if thumb and not thumb.startswith('http'): thumb = urljoin(url, thumb)
                
                title = a.get('title') or a.get_text(strip=True)
                results.append({
                    'url': href,
                    'thumb': thumb,
                    'title': title[:50]
                })
        return results

# --- BOT STATES ---
class Form(StatesGroup):
    waiting_for_category = State()
    waiting_for_quantity = State()
    scraping = State()

bot = Bot(token=config.BOT_TOKEN)
dp = Dispatcher()

def get_category_keyboard():
    buttons = []
    # Handle large category list (3 columns)
    for i in range(0, len(config.CATEGORIES), 3):
        row = []
        for j in range(3):
            if i + j < len(config.CATEGORIES):
                row.append(InlineKeyboardButton(text=config.CATEGORIES[i+j], callback_data=f"cat_{config.CATEGORIES[i+j]}"))
        if row:
            buttons.append(row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_quantity_keyboard():
    buttons = []
    row = []
    for qty in config.QUANTITIES:
        row.append(InlineKeyboardButton(text=str(qty), callback_data=f"qty_{qty}"))
        if len(row) == 4:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Select a category:", reply_markup=get_category_keyboard())
    await state.set_state(Form.waiting_for_category)

@dp.callback_query(F.data.startswith("cat_"), Form.waiting_for_category)
async def category_selected(callback: types.CallbackQuery, state: FSMContext):
    category = callback.data.split("cat_", 1)[1]
    await state.update_data(selected_category=category)
    await callback.message.edit_text(f"Category: <b>{category}</b>\n\nSelect quantity:", reply_markup=get_quantity_keyboard())
    await state.set_state(Form.waiting_for_quantity)
    await callback.answer()

@dp.callback_query(F.data.startswith("qty_"), Form.waiting_for_quantity)
async def quantity_selected(callback: types.CallbackQuery, state: FSMContext):
    qty = int(callback.data.split("qty_", 1)[1])
    user_data = await state.get_data()
    category = user_data.get('selected_category')
    
    await callback.message.edit_text(f"Starting...\n<b>{category}</b>\nTarget: {qty} videos.")
    await state.set_state(Form.scraping)
    
    # Start background task
    asyncio.create_task(run_scraping_task(callback.message, category, qty))
    await callback.answer()

async def run_scraping_task(message, category, target_qty):
    sent_count = 0
    processed_in_session = set()
    
    tasks = []
    for base in config.SOURCES:
        if "desixclip" in base:
            tasks.append((base, Scraper.scrape_desixclip))
        elif "kamababa" in base:
            tasks.append((base, Scraper.scrape_kamababa))
        elif "hindichudaivideos" in base:
            tasks.append((base, Scraper.scrape_hindichudaivideos))
        elif "mydesi2" in base:
            tasks.append((base, Scraper.scrape_mydesi2))
        elif "eporner" in base:
            tasks.append((base, Scraper.scrape_eporner))
        else:
            # Fallback to generic for the others (tamilsexzone, spicymms, etc)
            tasks.append((base, Scraper.scrape_generic))
            
    random.shuffle(tasks)

    async with aiohttp.ClientSession() as session:
        source_index = 0
        while sent_count < target_qty and source_index < len(tasks):
            url, scraper_func = tasks[source_index]
            source_index += 1
            
            logger.info(f"Scraping Source: {url}")
            
            try:
                data_list = await scraper_func(url, category, session)
                logger.info(f"Found {len(data_list)} items from {url}")
                
                # Limit per source to prevent getting stuck on one giant site
                items_processed_from_source = 0
                
                for item in data_list:
                    if sent_count >= target_qty: break
                    if items_processed_from_source > 50: break # Safety cap per page
                    
                    v_url = item.get('url')
                    if not v_url or len(v_url) < 10: continue
                        
                    if v_url in processed_in_session: continue
                    processed_in_session.add(v_url)
                    
                    if await is_duplicate(v_url): continue
                    
                    try:
                        chat_id = config.TARGET_GROUP_ID
                        v_title = item.get('title', 'Video')
                        v_thumb = item.get('thumb')
                        
                        # Sanitize title
                        safe_title = v_title.replace('<', '').replace('>', '')
                        clickable_link = f'<a href="{v_url}">{safe_title}</a>'
                        caption = f"<b>{category}</b>\n{clickable_link}"
                        
                        photo_sent = False
                        if v_thumb and v_thumb.startswith('http'):
                            try:
                                logger.info(f"Sending Photo: {v_thumb}")
                                await bot.send_photo(chat_id, v_thumb, caption=caption, parse_mode="HTML")
                                photo_sent = True
                            except Exception as photo_err:
                                logger.warning(f"Photo failed: {photo_err}")
                        
                        if not photo_sent:
                            logger.info(f"Sending Text: {v_url}")
                            await bot.send_message(chat_id, caption, parse_mode="HTML", disable_web_page_preview=True)
                        
                        await save_video(v_url, v_title, category)
                        sent_count += 1
                        
                        # RATE LIMITING IS CRITICAL FOR LARGE QUANTITIES (50-200)
                        # 3 to 5 seconds sleep to avoid 429 Flood Control
                        sleep_time = random.uniform(3.0, 5.0)
                        await asyncio.sleep(sleep_time)
                        
                    except Exception as e:
                        logger.error(f"Send Error: {e}")
                        if "Flood" in str(e) or "429" in str(e):
                            logger.warning("Flood control detected. Sleeping 30s.")
                            await asyncio.sleep(30)
                        
                    items_processed_from_source += 1

            except Exception as e:
                logger.error(f"Scraping Logic Error on {url}: {e}")

    try:
        await message.edit_text(f"Finished.\n<b>{category}</b>\nSent: {sent_count}/{target_qty}", parse_mode="HTML")
    except Exception:
        pass

# --- FASTAPI ---
app = FastAPI()

@app.on_event("startup")
async def startup_event():
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(db_executor, ensure_db_exists_sync)
    logger.info("Application Started")

@app.get("/")
async def health_check():
    return {"status": "ok"}

@app.post("/")
async def process_update(request: Request):
    data = await request.json()
    try:
        update = types.Update(**data)
        await dp.feed_update(bot, update)
    except Exception as e:
        logger.error(f"Error processing update: {e}")
    return Response(status_code=200)

# --- MAIN ---
async def main():
    port = int(os.environ.get("PORT", 8000))
    is_webhook_env = os.environ.get("PORT") is not None
    base_url = os.environ.get("APP_URL")

    if is_webhook_env and base_url:
        logger.info(f"Running in WEBHOOK mode on port {port}")
        webhook_url = f"{base_url}/"
        await bot.delete_webhook(drop_pending_updates=True)
        await bot.set_webhook(url=webhook_url)
        import uvicorn
        config_uv = uvicorn.Config(app, host="0.0.0.0", port=port)
        server = uvicorn.Server(config_uv)
        await server.serve()
    else:
        logger.info("Running in POLLING mode")
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped.")
