import asyncio
import logging
import sqlite3
import random
import os
from urllib.parse import urljoin
from contextlib import closing

import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from bs4 import BeautifulSoup

# FastAPI imports for Webhook mode
from fastapi import FastAPI, Request
from fastapi.responses import Response

import config

# --- LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- DATABASE HANDLING ---
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
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        try:
            async with session.get(url, headers=headers, timeout=20) as response:
                if response.status == 200:
                    return await response.text()
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
        return None

    @staticmethod
    async def extract_videos(url, category, session):
        html = await Scraper.get_page(url, session)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        videos = []

        # 1. Look for <video> tags
        for vid in soup.find_all('video'):
            src = vid.get('src')
            if src:
                videos.append({'url': urljoin(url, src), 'title': vid.get('title', 'Video')})

        # 2. Look for <a> tags ending in .mp4
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.endswith('.mp4'):
                videos.append({'url': urljoin(url, href), 'title': a.get_text(strip=True)[:50]})

        return list({v['url']: v for v in videos}.values())

# --- BOT STATES ---
class Form(StatesGroup):
    waiting_for_category = State()
    waiting_for_quantity = State()
    scraping = State()

# --- SETUP ---
bot = Bot(token=config.BOT_TOKEN)
dp = Dispatcher()

# --- KEYBOARDS ---
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
    
    await callback.message.edit_text(f"Scraping {qty} videos for <b>{category}</b>...\nPlease wait.")
    await state.set_state(Form.scraping)
    
    asyncio.create_task(run_scraping_task(callback.message, category, qty))
    await callback.answer()

async def run_scraping_task(message, category, target_qty):
    sent_count = 0
    processed_in_session = set()
    
    async with aiohttp.ClientSession() as session:
        sources = config.SOURCES.copy()
        random.shuffle(sources)

        while sent_count < target_qty:
            if not sources:
                break
            
            current_url = sources.pop(0)
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
                        await asyncio.sleep(0.1)
                    except Exception as e:
                        logger.error(f"Send error: {e}")

    await message.answer(f"Done.\nSent {sent_count} videos.")

# --- FASTAPI APP (WEBHOOK MODE) ---
app = FastAPI()

@app.get("/")
async def health_check():
    # This endpoint is required for Koyeb to check if the app is healthy
    return {"status": "ok"}

@app.post("/")
async def process_update(request: Request):
    """
    Receives updates from Telegram/Koyeb.
    NO SECRET CHECK.
    """
    data = await request.json()
    update = types.Update(**data)
    await dp.feed_update(bot, update)
    return Response(status_code=200)

# --- MAIN ENTRY POINT ---
async def main():
    init_db()
    
    # Detect environment
    port = int(os.environ.get("PORT", 8000))
    is_webhook_env = os.environ.get("PORT") is not None
    
    # Get URL from Environment Variable
    base_url = os.environ.get("APP_URL")

    if is_webhook_env:
        # WEBHOOK MODE (Koyeb)
        logger.info("Running in WEBHOOK mode")
        webhook_url = f"{base_url}/" if base_url else None
        
        await bot.delete_webhook(drop_pending_updates=True)
        
        if webhook_url:
            logger.info(f"Setting webhook to: {webhook_url}")
            await bot.set_webhook(url=webhook_url)
        else:
            logger.warning("APP_URL environment variable is missing. Webhook not set.")

        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=port)
    else:
        # POLLING MODE (Local)
        logger.info("Running in POLLING mode")
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped.")
