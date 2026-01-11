import asyncio
import logging
import sqlite3
import random
import os
import re
from urllib.parse import urljoin, quote
from contextlib import closing

import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from bs4 import BeautifulSoup

from fastapi import FastAPI, Request
from fastapi.responses import Response

import config

# --- LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- DATABASE ---
DB_NAME = "bot_database.db"

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with closing(get_db_connection()) as conn:
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

def is_duplicate(video_url):
    with closing(get_db_connection()) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM sent_videos WHERE video_url = ? LIMIT 1", (video_url,))
        return cursor.fetchone() is not None

def save_video(video_url, title, category):
    with closing(get_db_connection()) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO sent_videos (video_url, video_title, category) VALUES (?, ?, ?)",
                (video_url, title, category)
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

# --- SPECIFIC SCRAPERS ---

class Scraper:
    @staticmethod
    async def get_page(url, session):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://www.google.com/"
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
    async def scrape_generic(url, session):
        html = await Scraper.get_page(url, session)
        if not html: return []
        soup = BeautifulSoup(html, 'html.parser')
        videos = []
        # 1. Look for <video>
        for v in soup.find_all('video'):
            if v.get('src'): videos.append(urljoin(url, v.get('src')))
        # 2. Look for <a href="...mp4">
        for a in soup.find_all('a', href=True):
            if a['href'].endswith('.mp4'): videos.append(urljoin(url, a['href']))
        return videos

    @staticmethod
    async def scrape_desixclip(base_url, category, session):
        # Searches and scrapes desixclip.me
        search_url = f"{base_url}?s={quote(category)}"
        html = await Scraper.get_page(search_url, session)
        if not html: return []
        soup = BeautifulSoup(html, 'html.parser')
        videos = []
        # Desixclip specific: Look for items with class 'video-post' or similar
        for item in soup.find_all('article'): 
            a_tag = item.find('a', href=True)
            if a_tag:
                page_url = urljoin(base_url, a_tag['href'])
                # Go to the page to find the real video link
                page_html = await Scraper.get_page(page_url, session)
                if page_html:
                    page_soup = BeautifulSoup(page_html, 'html.parser')
                    # Look for source inside video tag
                    vid = page_soup.find('video')
                    if vid and vid.get('src'):
                        videos.append(urljoin(page_url, vid.get('src')))
        return videos

    @staticmethod
    async def scrape_kamababa(base_url, category, session):
        # Kamababa usually lists videos on homepage or category pages
        html = await Scraper.get_page(base_url, session)
        if not html: return []
        soup = BeautifulSoup(html, 'html.parser')
        videos = []
        # Look for links inside specific divs
        for a in soup.find_all('a', href=True):
            href = a['href']
            if '/video/' in href or len(href) > 20:
                page_url = urljoin(base_url, href)
                page_html = await Scraper.get_page(page_url, session)
                if page_html:
                    page_soup = BeautifulSoup(page_html, 'html.parser')
                    # Find direct video links
                    for source in page_soup.find_all('source'):
                        if source.get('src') and source.get('type') == 'video/mp4':
                            videos.append(urljoin(page_url, source.get('src')))
                    # Fallback to video tag
                    for vid in page_soup.find_all('video'):
                        if vid.get('src'): videos.append(urljoin(page_url, vid.get('src')))
        return videos

    @staticmethod
    async def scrape_eporner(base_url, category, session):
        # Eporner is harder. We just scrape generic MP4 links here.
        # A full eporner scraper requires parsing their JSON API.
        html = await Scraper.get_page(base_url, session)
        if not html: return []
        videos = []
        # Try to find video IDs on homepage and construct links (Basic approach)
        # This is a simplified scraper for Eporner due to complexity
        return await Scraper.scrape_generic(base_url, session)

# --- BOT STATES ---
class Form(StatesGroup):
    waiting_for_category = State()
    waiting_for_quantity = State()
    scraping = State()

bot = Bot(token=config.BOT_TOKEN)
dp = Dispatcher()

def get_category_keyboard():
    buttons = []
    for i in range(0, len(config.CATEGORIES), 2):
        row = []
        row.append(InlineKeyboardButton(text=config.CATEGORIES[i], callback_data=f"cat_{config.CATEGORIES[i]}"))
        if i + 1 < len(config.CATEGORIES):
            row.append(InlineKeyboardButton(text=config.CATEGORIES[i+1], callback_data=f"cat_{config.CATEGORIES[i+1]}"))
        buttons.append(row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_quantity_keyboard():
    buttons = []
    row = []
    for qty in config.QUANTITIES:
        row.append(InlineKeyboardButton(text=str(qty), callback_data=f"qty_{qty}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Welcome. Select a category:", reply_markup=get_category_keyboard())
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
    
    asyncio.create_task(run_scraping_task(callback.message, category, qty))
    await callback.answer()

async def run_scraping_task(message, category, target_qty):
    sent_count = 0
    processed_in_session = set()
    
    # Build a task list: (URL, Scraper_Function)
    tasks = []
    
    for base in config.SOURCES:
        if "desixclip" in base:
            tasks.append((f"{base}?s={quote(category)}", Scraper.scrape_desixclip))
        elif "kamababa" in base:
            # Kamababa often blocks searches, scrape homepage or tag
            tasks.append((base, Scraper.scrape_kamababa))
        elif "eporner" in base:
            tasks.append((base, Scraper.scrape_eporner))
        else:
            # Generic fallback
            tasks.append((base, Scraper.scrape_generic))
            
    random.shuffle(tasks)

    async with aiohttp.ClientSession() as session:
        while sent_count < target_qty:
            if not tasks: break
            
            url, scraper_func = tasks.pop(0)
            logger.info(f"Scraping: {url}")
            
            try:
                video_links = await scraper_func(url, category, session)
                
                for v_url in video_links:
                    if sent_count >= target_qty: break
                    
                    if v_url in processed_in_session: continue
                    processed_in_session.add(v_url)
                    
                    if is_duplicate(v_url): continue
                    
                    if save_video(v_url, "Video", category):
                        try:
                            await bot.send_video(config.TARGET_GROUP_ID, v_url, caption=f"<b>{category}</b>")
                            sent_count += 1
                            await asyncio.sleep(0.1)
                        except Exception as e:
                            logger.error(f"Send error: {e}")
            except Exception as e:
                logger.error(f"Scrape error on {url}: {e}")

    await message.answer(f"Done.\nSent {sent_count} videos.")

# --- FASTAPI ---
app = FastAPI()

@app.get("/")
async def health_check():
    return {"status": "ok"}

@app.post("/")
async def process_update(request: Request):
    data = await request.json()
    update = types.Update(**data)
    await dp.feed_update(bot, update)
    return Response(status_code=200)

# --- MAIN ---
async def main():
    init_db()
    port = int(os.environ.get("PORT", 8000))
    is_webhook_env = os.environ.get("PORT") is not None
    base_url = os.environ.get("APP_URL")

    if is_webhook_env:
        logger.info("Running in WEBHOOK mode")
        webhook_url = f"{base_url}/" if base_url else None
        await bot.delete_webhook(drop_pending_updates=True)
        if webhook_url:
            await bot.set_webhook(url=webhook_url)
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=port)
    else:
        logger.info("Running in POLLING mode")
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped.")
