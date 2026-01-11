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

def init_db():
    # Force create/open the database
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
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
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"DB Init Error: {e}")
    finally:
        conn.close()

def is_duplicate(video_url):
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM sent_videos WHERE video_url = ? LIMIT 1", (video_url,))
        result = cursor.fetchone()
        conn.close()
        return result is not None
    except Exception as e:
        logger.error(f"DB Read Error: {e}")
        return False

def save_video(video_url, title, category):
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO sent_videos (video_url, video_title, category) VALUES (?, ?, ?)",
            (video_url, title, category)
        )
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        return False
    except Exception as e:
        logger.error(f"DB Write Error: {e}")
        return False

# --- SCRAPERS ---

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
                    if response.status != 403:
                        logger.warning(f"HTTP {response.status} on {url}")
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
        return None

    @staticmethod
    async def scrape_generic(url, category, session):
        html = await Scraper.get_page(url, session)
        if not html: return []
        soup = BeautifulSoup(html, 'html.parser')
        results = []
        
        # Find links that look like video pages
        for a in soup.find_all('a', href=True):
            href = a['href']
            # Heuristic: links containing video/post/numbers
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
        html = await Scraper.get_page(url, session)
        if not html: return []
        soup = BeautifulSoup(html, 'html.parser')
        results = []
        for item in soup.find_all('article'):
            a_tag = item.find('a', href=True)
            if a_tag:
                img_tag = item.find('img')
                thumb = img_tag.get('src') if img_tag else None
                if thumb and not thumb.startswith('http'):
                    thumb = urljoin(url, thumb)
                
                results.append({
                    'url': urljoin(url, a_tag['href']),
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
        for a in soup.find_all('a', href=True):
            href = a['href']
            if '/video/' in href:
                img = a.find('img')
                thumb = img.get('src') if img else None
                if thumb and not thumb.startswith('http'):
                    thumb = urljoin(url, thumb)
                results.append({
                    'url': urljoin(url, href),
                    'thumb': thumb,
                    'title': a.get_text(strip=True)[:50]
                })
        return results

    @staticmethod
    async def scrape_eporner(url, category, session):
        return await Scraper.scrape_generic(url, category, session)

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
    
    tasks = []
    for base in config.SOURCES:
        if "desixclip" in base:
            separator = "&" if "?" in base else "?"
            tasks.append((f"{base}{separator}s={quote(category)}", Scraper.scrape_desixclip))
        elif "kamababa" in base:
            tasks.append((base, Scraper.scrape_kamababa))
        elif "eporner" in base:
            tasks.append((base, Scraper.scrape_eporner))
        else:
            tasks.append((base, Scraper.scrape_generic))
            
    random.shuffle(tasks)

    async with aiohttp.ClientSession() as session:
        while sent_count < target_qty:
            if not tasks: break
            
            url, scraper_func = tasks.pop(0)
            logger.info(f"--- Scraping URL: {url} ---")
            
            try:
                data_list = await scraper_func(url, category, session)
                logger.info(f"Found {len(data_list)} items from {url}")
                
                for item in data_list:
                    if sent_count >= target_qty: break
                    
                    v_url = item['url']
                    v_title = item.get('title', 'Video')
                    v_thumb = item.get('thumb')
                    
                    if not v_url or len(v_url) < 10: continue
                        
                    if v_url in processed_in_session: continue
                    processed_in_session.add(v_url)
                    
                    if is_duplicate(v_url): continue
                    
                    if save_video(v_url, v_title, category):
                        try:
                            chat_id = int(config.TARGET_GROUP_ID)
                            
                            # CORRECTED HTML ANCHOR TAG
                            clickable_link = f'<a href="{v_url}">{v_title}</a>'
                            caption = f"<b>{category}</b>\n{clickable_link}"
                            
                            # 1. Try Sending Photo
                            photo_sent = False
                            if v_thumb and v_thumb.startswith('http'):
                                try:
                                    logger.info(f"Sending Photo: {v_thumb}")
                                    await bot.send_photo(chat_id, v_thumb, caption=caption, parse_mode="HTML")
                                    photo_sent = True
                                except Exception as photo_err:
                                    logger.warning(f"Photo failed: {photo_err}. Sending text instead.")
                            
                            # 2. Fallback to Text Message
                            if not photo_sent:
                                logger.info(f"Sending Text: {v_url}")
                                await bot.send_message(chat_id, caption, parse_mode="HTML")
                            
                            sent_count += 1
                            await asyncio.sleep(0.2)
                            
                        except Exception as e:
                            logger.error(f"CRITICAL Send error: {e}")
            except Exception as e:
                logger.error(f"Scrape loop error on {url}: {e}")

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
    # Initialize DB immediately on startup
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
