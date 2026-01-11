import asyncio
import logging
import sqlite3
import random
import os
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

# --- SCRAPER ---
class Scraper:
    @staticmethod
    async def get_page(url, session):
        # Simulating a real browser to avoid getting blocked immediately
        headers = {
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": "https://www.google.com/"
        }
        try:
            async with session.get(url, headers=headers, timeout=15) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    logger.warning(f"Status {response.status} for {url}")
        except Exception as e:
            logger.error(f"Fetch error {url}: {e}")
        return None

    @staticmethod
    async def extract_videos(url, category, session):
        html = await Scraper.get_page(url, session)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        videos = []

        # STRATEGY 1: Look for standard <video> tags
        for vid in soup.find_all('video'):
            src = vid.get('src')
            poster = vid.get('poster')
            if src:
                videos.append({'url': urljoin(url, src), 'title': 'Video Found'})

        # STRATEGY 2: Look for links ending in .mp4
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.endswith('.mp4'):
                videos.append({'url': urljoin(url, href), 'title': a.get_text(strip=True)[:50]})

        # STRATEGY 3: Look for specific generic classes (Common in tube sites)
        # This is a "best guess" scraper.
        for link in soup.find_all('a', href=True):
            href = link['href']
            # If the link looks like a video page (long string)
            if len(href) > 10 and not href.endswith('.html') and not href.endswith('.php'):
                 # We will check if this looks like a direct video or a page
                 # For this simple bot, we mostly trust direct .mp4 or video tags
                 pass

        logger.info(f"Scraped {url}: Found {len(videos)} potential videos.")
        return list({v['url']: v for v in videos}.values())

# --- BOT STATES ---
class Form(StatesGroup):
    waiting_for_category = State()
    waiting_for_quantity = State()
    scraping = State()

# --- SETUP ---
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

# --- HANDLERS ---
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
    
    # Update sources to be Search Queries based on category
    # This dynamically creates search URLs for the sites provided
    sources_to_try = []
    
    # Helper to build search URLs
    for base in config.SOURCES:
        # If the site looks like it supports search query
        if "desixclip" in base:
            sources_to_try.append(f"https://desixclip.me/?s={quote(category)}")
        elif "kamababa" in base:
            sources_to_try.append(f"https://www.kamababa.desi/?s={quote(category)}")
        elif "tamilsexzone" in base:
            sources_to_try.append(f"https://www.tamilsexzone.com/?s={quote(category)}")
        else:
            # Fallback to the homepage if we don't know the search structure
            sources_to_try.append(base)

    async with aiohttp.ClientSession() as session:
        random.shuffle(sources_to_try)

        while sent_count < target_qty:
            if not sources_to_try:
                break
            
            current_url = sources_to_try.pop(0)
            logger.info(f"Scanning: {current_url}")
            
            videos = await Scraper.extract_videos(current_url, category, session)
            
            for video in videos:
                if sent_count >= target_qty:
                    break
                
                v_url = video['url']
                v_title = video['title']
                
                if v_url in processed_in_session:
                    continue
                processed_in_session.add(v_url)
                
                if is_duplicate(v_url):
                    continue
                
                if save_video(v_url, v_title, category):
                    try:
                        caption = f"<b>{category}</b>\n{v_title}"
                        await bot.send_video(config.TARGET_GROUP_ID, v_url, caption=caption)
                        sent_count += 1
                        logger.info(f"Sent [{sent_count}/{target_qty}]: {v_url}")
                        await asyncio.sleep(0.1)
                    except Exception as e:
                        logger.error(f"Send error: {e}")

    await message.answer(f"Done.\nSent {sent_count} videos.")

# --- FASTAPI APP ---
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
            logger.info(f"Setting webhook to: {webhook_url}")
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
