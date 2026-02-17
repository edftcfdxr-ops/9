import argparse
import json
import os
import time
import random
import logging
import unicodedata
import sqlite3
import re
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth
import urllib.parse
import subprocess
import pty
import errno
import sys
from typing import Dict, List
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import threading
import uuid
import signal
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ConversationHandler, ContextTypes
import asyncio
from dotenv import load_dotenv
import psutil
from queue import Queue, Empty

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('instagram_bot.log'),
        logging.StreamHandler()
    ]
)

user_fetching = set()
user_cancel_fetch = set()
AUTHORIZED_FILE = 'authorized_users.json'
TASKS_FILE = 'tasks.json'
OWNER_TG_ID = int(os.environ.get('OWNER_TG_ID'))
BOT_TOKEN = os.environ.get('BOT_TOKEN')
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
MOBILE_UA = "Mozilla/5.0 (Linux; Android 13; vivo V60) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36"

authorized_users = []
users_data: Dict[int, Dict] = {}
users_pending: Dict[int, Dict] = {}
users_tasks: Dict[int, List[Dict]] = {}
persistent_tasks = []
running_processes: Dict[int, subprocess.Popen] = {}
waiting_for_otp = {}
user_queues = {}

# Global pbattack control
pbattack_stop_events = {}  # Dict[int, threading.Event]
pbattack_queues = {}       # Dict[int, Queue]
pbattack_active = {}       # Dict[int, bool]

# Ensure sessions directory exists
os.makedirs('sessions', exist_ok=True)

# ===== PLAYWRIGHT HELPER: Apply Stealth to Page =====
def apply_stealth_to_page_sync(page):
    """Apply stealth mode to a Playwright sync page"""
    try:
        stealth = Stealth()
        stealth.apply_stealth_sync(page)
        logging.info("✅ Stealth applied to page")
    except Exception as e:
        logging.warning(f"⚠️ Could not apply stealth: {e}")

async def apply_stealth_to_page_async(page):
    """Apply stealth mode to a Playwright async page"""
    try:
        stealth = Stealth()
        await stealth.apply_stealth_async(page)
        logging.info("✅ Stealth applied to async page")
    except Exception as e:
        logging.warning(f"⚠️ Could not apply stealth: {e}")

# ===== ASYNC PLAYWRIGHT LOGIN =====
async def playwright_login_and_save_state(username: str, password: str, user_id: int) -> str:
    """
    Pure Playwright login without any API libraries.
    Logs in to Instagram and saves storage state for reuse.
    """
    COOKIE_FILE = f"sessions/{user_id}_{username}_state.json"

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-gpu",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )

        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 720},
        )

        page = await context.new_page()
        
        # Apply stealth
        await apply_stealth_to_page_async(page)

        login_url = "https://www.instagram.com/accounts/login/?__coig_login=1"
        logging.info("[PLOGIN] Navigating to %s", login_url)

        await page.goto(login_url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(3000)

        logging.info("[PLOGIN] URL=%s", page.url)

        # Check for login form
        username_inputs = await page.locator('input[name="username"]').count()
        if username_inputs == 0:
            logging.warning("[PLOGIN] Username field not found, waiting...")
            await page.wait_for_timeout(5000)
            username_inputs = await page.locator('input[name="username"]').count()

        if username_inputs == 0:
            logging.error("[PLOGIN] Login form NOT loaded. URL=%s", page.url)
            await browser.close()
            raise ValueError("ERROR_010: Instagram login form not loaded")

        # Perform human-like login
        username_input = page.locator('input[name="username"]')
        password_input = page.locator('input[name="password"]')
        login_button = page.locator('button[type="submit"]').first

        # Username input with delay
        await username_input.click()
        await page.wait_for_timeout(random.randint(300, 900))
        await username_input.fill("")
        await username_input.type(username, delay=random.randint(60, 140))

        # Password input with delay
        await page.wait_for_timeout(random.randint(300, 900))
        await password_input.click()
        await page.wait_for_timeout(random.randint(200, 700))
        await password_input.fill("")
        await password_input.type(password, delay=random.randint(60, 140))

        # Submit
        await page.wait_for_timeout(random.randint(400, 1000))
        await login_button.click()
        logging.info("[PLOGIN] Login form submitted for %s", username)

        # Wait for redirect
        await page.wait_for_timeout(5000)
        current_url = page.url
        logging.info("[PLOGIN] After login URL=%s", current_url)

        # Check for OTP/challenge
        otp_count = await page.locator('input[name="verificationCode"]').count()
        if otp_count > 0 or "challenge" in current_url or "two_factor" in current_url:
            logging.warning("[PLOGIN] OTP/challenge detected for %s", username)
            await browser.close()
            raise ValueError("ERROR_OTP: OTP/challenge required - handle via 2FA flow")

        logging.info("[PLOGIN] Login successful for %s", username)
        await page.wait_for_timeout(4000)

        # Save storage state for future use
        await context.storage_state(path=COOKIE_FILE)
        logging.info("[PLOGIN] Storage state saved to %s", COOKIE_FILE)

        await browser.close()

    return COOKIE_FILE

# ===== PLAYWRIGHT-BASED GROUP CHATS DISCOVERY =====
async def list_group_chats_playwright(username: str, storage_state: Dict, max_groups: int = 10) -> List[Dict]:
    """
    Discover group chats using Playwright browser automation.
    Navigates to DM inbox and extracts group thread URLs.
    """
    groups = []
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--disable-gpu",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )

            context = await browser.new_context(
                user_agent=MOBILE_UA,
                viewport={"width": 412, "height": 915},
                storage_state=storage_state,
                is_mobile=True,
            )

            page = await context.new_page()
            
            # Apply stealth
            await apply_stealth_to_page_async(page)

            logging.info("[GC-DISCOVER] Loading DM inbox for %s", username)
            await page.goto("https://www.instagram.com/direct/inbox/", timeout=60000)
            
            # Wait for inbox to load
            try:
                await page.wait_for_selector('a[href*="/direct/t/"], div[role="button"]', timeout=30000)
            except Exception as e:
                logging.warning("[GC-DISCOVER] Inbox load timeout: %s", e)
                await browser.close()
                return []

            await page.wait_for_timeout(3000)

            # Scroll to load more threads
            for _ in range(5):
                try:
                    await page.evaluate("window.scrollBy(0, 300)")
                    await page.wait_for_timeout(800)
                except:
                    pass

            # Extract group chat threads using JavaScript
            try:
                threads_data = await page.evaluate("""
                    () => {
                        const threads = [];
                        const seen = new Set();
                        
                        // Find all DM thread links
                        document.querySelectorAll('a[href*="/direct/t/"]').forEach(el => {
                            let href = el.getAttribute('href');
                            if (href && !seen.has(href)) {
                                // Get thread name/display
                                let text = el.innerText || el.textContent || '';
                                text = text.trim().substring(0, 100);
                                if (text) {
                                    seen.add(href);
                                    threads.push({
                                        display: text,
                                        url: 'https://www.instagram.com' + href
                                    });
                                }
                            }
                        });
                        return threads;
                    }
                """)
                groups = threads_data[:max_groups] if threads_data else []
                logging.info("[GC-DISCOVER] Found %d group threads", len(groups))
            except Exception as e:
                logging.error("[GC-DISCOVER] Error extracting threads: %s", e)

            await browser.close()

    except Exception as e:
        logging.error("[GC-DISCOVER] Exception: %s", e)

    return groups

# ===== PLAYWRIGHT-BASED DM THREAD URL DISCOVERY =====
async def get_dm_thread_url_playwright(username: str, storage_state: Dict, target_username: str) -> str:
    """
    Find DM thread URL for a target user using Playwright.
    Searches for existing DM or initiates new one.
    """
    thread_url = None
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--disable-gpu",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )

            context = await browser.new_context(
                user_agent=MOBILE_UA,
                viewport={"width": 412, "height": 915},
                storage_state=storage_state,
                is_mobile=True,
            )

            page = await context.new_page()
            
            # Apply stealth
            await apply_stealth_to_page_async(page)

            logging.info("[DM-DISCOVER] Looking for DM thread with %s", target_username)
            
            # Go to DM inbox
            await page.goto("https://www.instagram.com/direct/inbox/", timeout=60000)
            await page.wait_for_timeout(2000)

            # Try to find existing thread in inbox  
            thread_url = await page.evaluate(f"""
                () => {{
                    const target = '{target_username.lower()}';
                    let found = null;
                    document.querySelectorAll('a[href*="/direct/t/"]').forEach(el => {{
                        let text = (el.innerText || el.textContent || '').toLowerCase();
                        if (text.includes(target) && !found) {{
                            found = el.getAttribute('href');
                        }}
                    }});
                    if (found) return 'https://www.instagram.com' + found;
                    return null;
                }}
            """)

            if not thread_url:
                # Try to navigate to username profile and start DM
                logging.info("[DM-DISCOVER] Thread not in inbox, trying to start new DM")
                profile_url = f"https://www.instagram.com/{target_username}/"
                try:
                    await page.goto(profile_url, timeout=60000)
                    await page.wait_for_timeout(2000)
                    
                    # Look for message button
                    msg_button = page.locator('button:has-text("Message"), a:has-text("Message")')
                    if await msg_button.count() > 0:
                        await msg_button.first.click()
                        await page.wait_for_timeout(3000)
                        
                        # Get the thread URL after click
                        current_url = page.url
                        if "/direct/t/" in current_url:
                            thread_url = current_url
                            logging.info("[DM-DISCOVER] Started new DM, got URL: %s", thread_url)
                except Exception as e:
                    logging.warning("[DM-DISCOVER] Could not start new DM: %s", e)

            await browser.close()

    except Exception as e:
        logging.error("[DM-DISCOVER] Exception: %s", e)

    if thread_url:
        logging.info("[DM-DISCOVER] Found/created DM thread: %s", thread_url)
    else:
        logging.warning("[DM-DISCOVER] Could not find DM thread for %s", target_username)
    
    return thread_url

# ===== SYNC WRAPPER FOR PLAYWRIGHT (runs in thread) =====
def run_with_sync_playwright(fn, *args, **kwargs):
    """
    Runs `fn(p, *args, **kwargs)` where p is sync_playwright()
    """
    result = {"value": None, "exc": None}

    def target():
        try:
            with sync_playwright() as p:
                result["value"] = fn(p, *args, **kwargs)
        except Exception as e:
            result["exc"] = e

    t = threading.Thread(target=target)
    t.start()
    t.join()
    if result["exc"]:
        raise result["exc"]
    return result["value"]

# ===== LOAD/SAVE FUNCTIONS =====
def load_authorized():
    global authorized_users
    if os.path.exists(AUTHORIZED_FILE):
        with open(AUTHORIZED_FILE, 'r') as f:
            authorized_users = json.load(f)
    if not any(u['id'] == OWNER_TG_ID for u in authorized_users):
        authorized_users.append({'id': OWNER_TG_ID, 'username': 'owner'})

load_authorized()

def load_users_data():
    global users_data
    users_data = {}
    for file in os.listdir('.'):
        if file.startswith('user_') and file.endswith('.json'):
            user_id_str = file[5:-5]
            if user_id_str.isdigit():
                user_id = int(user_id_str)
                with open(file, 'r') as f:
                    data = json.load(f)
                if 'pairs' not in data:
                    data['pairs'] = None
                if 'switch_minutes' not in data:
                    data['switch_minutes'] = 10
                if 'threads' not in data:
                    data['threads'] = 1
                users_data[user_id] = data

load_users_data()

def save_authorized():
    with open(AUTHORIZED_FILE, 'w') as f:
        json.dump(authorized_users, f)

def save_user_data(user_id: int, data: Dict):
    with open(f'user_{user_id}.json', 'w') as f:
        json.dump(data, f)

def is_authorized(user_id: int) -> bool:
    return any(u['id'] == user_id for u in authorized_users)

def is_owner(user_id: int) -> bool:
    return user_id == OWNER_TG_ID

# ===== PTY-BASED PLAYWRIGHT LOGIN (for Telegram) =====
def child_playwright_login(user_id: int, username: str, password: str):
    """
    Child process: Async Playwright login using asyncio.run
    """
    try:
        print(f"[{username}] ⚙️ Starting Playwright browser login...")
        
        # Run async login in this child process
        state_file = asyncio.run(playwright_login_and_save_state(username, password, user_id))
        
        # Load state and print success
        with open(state_file, 'r') as f:
            state = json.load(f)
        
        print(f"[{username}] ✅ Login successful! Storage state saved.")
    except ValueError as ve:
        print(f"[{username}] ❌ Login error: {str(ve)}")
    except Exception as e:
        print(f"[{username}] ❌ Unexpected error: {e}")
    finally:
        time.sleep(0.5)
        sys.exit(0)

def reader_thread(user_id: int, chat_id: int, master_fd: int, username: str, password: str):
    """
    PTY reader thread for login output
    """
    global APP, LOOP
    buf = b""
    while True:
        try:
            data = os.read(master_fd, 1024)
            if not data:
                break
            buf += data
            while b"\n" in buf or len(buf) > 2048:
                if b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    text = line.decode(errors="ignore").strip()
                else:
                    text = buf.decode(errors="ignore")
                    buf = b""
                if not text:
                    continue
                lower = text.lower()
                if (
                    len(text) > 300
                    or "cdninstagram.com" in lower
                    or "http" in lower
                    or "{" in text
                    or "}" in text
                ):
                    continue
                try:
                    if APP and LOOP:
                        asyncio.run_coroutine_threadsafe(
                            APP.bot.send_message(chat_id=chat_id, text=f"🔥 {text}"), LOOP
                        )
                except Exception:
                    logging.error("[THREAD] send_message failed")
        except OSError as e:
            if e.errno == errno.EIO:
                break
            else:
                logging.error("[THREAD] PTY read error: %s", e)
                break
        except Exception as e:
            logging.error("[THREAD] Unexpected error: %s", e)
            break
    
    try:
        playwright_file = f"sessions/{user_id}_{username}_state.json"
        if os.path.exists(playwright_file):
            with open(playwright_file, 'r') as f:
                state = json.load(f)
            if user_id in users_data:
                data = users_data[user_id]
            else:
                data = {'accounts': [], 'default': None, 'pairs': None, 'switch_minutes': 10, 'threads': 1}
            
            norm_username = username.strip().lower()
            
            for i, acc in enumerate(data['accounts']):
                if acc.get('ig_username', '').strip().lower() == norm_username:
                    data['accounts'][i] = {'ig_username': norm_username, 'password': password, 'storage_state': state}
                    data['default'] = i
                    break
            else:
                data['accounts'].append({'ig_username': norm_username, 'password': password, 'storage_state': state})
                data['default'] = len(data['accounts']) - 1
            
            save_user_data(user_id, data)
            users_data[user_id] = data
            if APP and LOOP:
                asyncio.run_coroutine_threadsafe(APP.bot.send_message(chat_id=chat_id, text="✅ Login successful & saved! 🎉"), LOOP)
        else:
            if APP and LOOP:
                asyncio.run_coroutine_threadsafe(APP.bot.send_message(chat_id=chat_id, text="⚠️ Login failed. No session saved."), LOOP)
    except Exception as e:
        logging.error("Failed to save user data: %s", e)
        if APP and LOOP:
            asyncio.run_coroutine_threadsafe(APP.bot.send_message(chat_id=chat_id, text=f"⚠️ Error: {str(e)}"), LOOP)
    finally:
        with SESSIONS_LOCK:
            if user_id in SESSIONS:
                try:
                    os.close(SESSIONS[user_id]["master_fd"])
                except Exception:
                    pass
                SESSIONS.pop(user_id, None)

# ===== GLOBALS FOR PTY =====
APP = None
LOOP = None
SESSIONS = {}
SESSIONS_LOCK = threading.Lock()

# ===== RELAY INPUT =====
async def relay_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text
    with SESSIONS_LOCK:
        info = SESSIONS.get(user_id)
    if not info:
        return
    master_fd = info["master_fd"]
    try:
        os.write(master_fd, (text + "\n").encode())
    except OSError as e:
        await update.message.reply_text(f"Failed: {e}")

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()
    if user_id in waiting_for_otp:
        if len(text) == 6 and text.isdigit():
            user_queues[user_id].put(text)
            del waiting_for_otp[user_id]
            await update.message.reply_text("✅ Code submitted!")
            return
        else:
            await update.message.reply_text("❌ Enter 6-digit code")
            return
    await relay_input(update, context)

# ===== TELEGRAM COMMAND HANDLERS =====
async def cmd_kill(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    with SESSIONS_LOCK:
        info = SESSIONS.get(user_id)
    if not info:
        await update.message.reply_text("No active PTY session.")
        return
    pid = info["pid"]
    master_fd = info["master_fd"]
    try:
        os.kill(pid, 15)
    except Exception:
        pass
    try:
        os.close(master_fd)
    except Exception:
        pass
    with SESSIONS_LOCK:
        SESSIONS.pop(user_id, None)
    await update.message.reply_text(f"🛑 Stopped session (pid={pid}).")

async def flush(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_owner(user_id):
        await update.message.reply_text("⚠️ Admin only ⚠️")
        return
    global users_tasks, persistent_tasks
    count = 0
    for uid, tasks in users_tasks.items():
        for task in tasks[:]:
            proc = task['proc']
            proc.terminate()
            await asyncio.sleep(3)
            if proc.poll() is None:
                proc.kill()
            if 'names_file' in task and os.path.exists(task['names_file']):
                os.remove(task['names_file'])
            mark_task_stopped_persistent(task['id'])
            tasks.remove(task)
            count += 1
        users_tasks[uid] = tasks
    await update.message.reply_text(f"🛑 Stopped {count} tasks!")

# ===== PBATTACK: WORKER-BASED MESSAGE SENDER WITH QUEUE =====

async def playwright_pbattack_worker(
    worker_id: int,
    user_id: int,
    chat_link: str,
    message_queue: Queue,
    stop_event: threading.Event,
    storage_state: dict,
    stats: dict,
    messages: list
) -> None:
    """
    Worker thread that processes messages from queue and sends them in infinite loop.
    Multiple workers can run in parallel.
    
    Args:
        worker_id: Unique ID for this worker (1, 2, 3, or 4)
        user_id: Telegram user ID
        chat_link: Instagram group chat URL
        message_queue: Queue with messages to send
        stop_event: threading.Event to signal stop
        storage_state: Playwright storage state
        stats: dict to track sent/failed counts
        messages: Original message list for infinite loop
    """
    try:
        logging.info(f"[PBATTACK] Worker {worker_id} starting for user {user_id}")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--disable-gpu",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                ],
            )
            
            context = await browser.new_context(
                user_agent=MOBILE_UA,
                viewport={"width": 1280, "height": 720},
            )
            
            # Add cookies from storage state
            if storage_state and 'cookies' in storage_state:
                await context.add_cookies(storage_state['cookies'])
            
            page = await context.new_page()
            
            # Apply stealth
            await apply_stealth_to_page_async(page)
            
            # Navigate to group chat
            try:
                await page.goto(chat_link, timeout=30000, wait_until="domcontentloaded")
            except Exception as e:
                logging.error(f"[PBATTACK] Worker {worker_id} failed to navigate: {e}")
                await browser.close()
                return
            
            # Input box selectors
            input_selectors = [
                'input[placeholder*="message"]',
                'input[aria-label*="message"]',
                'textarea[placeholder*="message"]',
                'div[contenteditable="true"]',
                'input[type="text"]',
            ]
            
            # Infinite loop - keeps sending until stop_event
            while not stop_event.is_set():
                try:
                    # Get message from queue with timeout
                    msg = message_queue.get(timeout=0.1)
                except Empty:
                    # Queue empty? Refill with all messages again for infinite loop
                    if not stop_event.is_set():
                        for m in messages:
                            message_queue.put(m)
                    continue
                
                if msg is None:  # Poison pill - worker should exit
                    break
                
                # Try to send message with retries
                sent = False
                retries = 2
                
                for attempt in range(retries):
                    try:
                        # Find input box
                        input_element = None
                        for selector in input_selectors:
                            try:
                                input_element = await page.query_selector(selector)
                                if input_element:
                                    break
                            except:
                                pass
                        
                        if not input_element:
                            logging.warning(f"[PBATTACK] Worker {worker_id}: Input box not found (attempt {attempt + 1})")
                            await asyncio.sleep(0.01)
                            continue
                        
                        # Click to focus the input
                        await input_element.click()
                        await asyncio.sleep(0.005)  # Quick focus settle
                        
                        # Clear any existing text
                        await input_element.evaluate("el => el.textContent = ''")
                        
                        # Type message with ULTRA-FAST delay (0.001 ms per char)
                        # This is fast enough to not be noticeable but DOES trigger send handlers
                        await input_element.type(msg, delay=0.001)
                        
                        # Minimal pause before sending
                        await asyncio.sleep(0.002)
                        
                        # Send with Enter
                        await page.keyboard.press("Enter")
                        
                        # Wait for message to register
                        await asyncio.sleep(0.01)
                        
                        logging.info(f"[PBATTACK] Worker {worker_id} sent: {msg[:30]}...")
                        sent = True
                        stats['sent'] += 1
                        break
                        
                    except Exception as e:
                        logging.warning(f"[PBATTACK] Worker {worker_id} error (attempt {attempt + 1}): {e}")
                        if attempt < retries - 1:
                            await asyncio.sleep(0.01)
                        continue
                
                if not sent:
                    logging.error(f"[PBATTACK] Worker {worker_id} failed to send after {retries} retries")
                    stats['failed'] += 1
            
            await browser.close()
            logging.info(f"[PBATTACK] Worker {worker_id} stopped")
    
    except Exception as e:
        logging.error(f"[PBATTACK] Worker {worker_id} exception: {e}")

async def playwright_pbattack_coordinator(
    chat_link: str,
    messages: list,
    user_id: int,
    storage_state: dict,
    num_workers: int = 2
) -> tuple:
    """
    Coordinator that spawns workers and distributes messages via queue.
    Workers loop infinitely, repeating messages until /stoppb is sent.
    
    Args:
        chat_link: Instagram chat URL
        messages: List of messages to send
        user_id: Telegram user ID
        storage_state: Playwright auth state
        num_workers: Number of worker threads (3-4 recommended)
    
    Returns:
        (success: bool, message: str, stats: dict)
    """
    try:
        logging.info(f"[PBATTACK] Starting coordinator with {num_workers} workers (INFINITE LOOP MODE)")
        
        # Create queue and stop event
        message_queue = Queue()
        stop_event = threading.Event()
        
        # Randomize initial message order
        shuffled_messages = messages.copy()
        random.shuffle(shuffled_messages)
        
        # Put messages in queue initially
        for msg in shuffled_messages:
            message_queue.put(msg)
        
        # Stats tracking
        stats = {
            'sent': 0,
            'failed': 0,
            'total': len(messages)
        }
        
        # Store for stop command
        pbattack_stop_events[user_id] = stop_event
        pbattack_queues[user_id] = message_queue
        pbattack_active[user_id] = True
        
        # Create and start worker tasks
        worker_tasks = []
        for worker_id in range(1, num_workers + 1):
            task = asyncio.create_task(
                playwright_pbattack_worker(
                    worker_id=worker_id,
                    user_id=user_id,
                    chat_link=chat_link,
                    message_queue=message_queue,
                    stop_event=stop_event,
                    storage_state=storage_state,
                    stats=stats,
                    messages=messages  # Pass original messages for infinite loop
                )
            )
            worker_tasks.append(task)
            logging.info(f"[PBATTACK] Spawned worker {worker_id}")
        
        # Wait for stop signal - NO max time limit
        # Workers keep running and repeating messages until /stoppb is sent
        while not stop_event.is_set():
            await asyncio.sleep(1)  # Light check
        
        logging.info("[PBATTACK] Stop signal received, stopping all workers")
        
        # Wait for workers to finish gracefully
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        
        # Cleanup
        pbattack_active[user_id] = False
        
        result_msg = f"✅ Stopped. Sent {stats['sent']} messages total"
        if stats['failed'] > 0:
            result_msg += f" ({stats['failed']} failed)"
        
        logging.info(f"[PBATTACK] Coordinator finished: {result_msg}")
        return (True, result_msg, stats)
        
    except Exception as e:
        logging.error(f"[PBATTACK] Coordinator error: {e}")
        pbattack_active[user_id] = False
        return (False, f"❌ Error: {str(e)}", {'sent': 0, 'failed': 0, 'total': 0})

# ===== COOKIE-BASED LOGIN (sessionid) =====
async def playwright_cookie_login(sessionid: str, user_id: int, username: str) -> tuple:
    """
    Pure Playwright login using Instagram sessionid cookie.
    
    Args:
        sessionid: Instagram sessionid cookie value
        user_id: Telegram user ID
        username: Instagram username (for file naming)
    
    Returns:
        (success: bool, message: str, storage_state: dict or None)
    """
    COOKIE_FILE = f"sessions/{user_id}_{username}_state.json"
    
    try:
        logging.info("[PBLOGIN] Starting cookie-based login for %s", username)
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--disable-gpu",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                ],
            )

            # Create context with the sessionid cookie
            context = await browser.new_context(
                user_agent=USER_AGENT,
                viewport={"width": 1280, "height": 720},
            )

            # Add sessionid cookie
            await context.add_cookies([
                {
                    "name": "sessionid",
                    "value": sessionid,
                    "domain": ".instagram.com",
                    "path": "/",
                    "httpOnly": True,
                    "secure": True,
                    "sameSite": "Lax",
                }
            ])

            page = await context.new_page()
            
            # Apply stealth
            await apply_stealth_to_page_async(page)
            logging.info("[PBLOGIN] Stealth applied")
            
            # Add human-like delays
            await page.wait_for_timeout(random.randint(1000, 3000))
            
            # Navigate to Instagram
            logging.info("[PBLOGIN] Navigating to instagram.com...")
            try:
                await page.goto("https://www.instagram.com/", timeout=60000, wait_until="domcontentloaded")
            except Exception as e:
                logging.warning("[PBLOGIN] Navigation timeout: %s", e)
            
            # Wait for page to settle
            await page.wait_for_timeout(3000)
            
            current_url = page.url
            logging.info("[PBLOGIN] Current URL: %s", current_url)
            
            # Check if login was successful
            # If we're redirected to login page, session is invalid
            if "accounts/login" in current_url:
                logging.warning("[PBLOGIN] Redirected to login page - invalid sessionid")
                await browser.close()
                return (False, "❌ Invalid sessionid - redirected to login", None)
            
            # Check for common success indicators
            try:
                # Wait for feed to load or profile to be accessible
                await page.wait_for_selector('svg[aria-label="Home"], a[href="/"]', timeout=10000)
                logging.info("[PBLOGIN] Instagram loaded successfully")
            except Exception as e:
                logging.warning("[PBLOGIN] Could not verify page load: %s", e)
            
            # Add final delay before saving
            await page.wait_for_timeout(random.randint(1000, 2000))
            
            # Save storage state for future use
            await context.storage_state(path=COOKIE_FILE)
            logging.info("[PBLOGIN] Storage state saved to %s", COOKIE_FILE)
            
            # Load and return storage state
            with open(COOKIE_FILE, 'r') as f:
                storage_state = json.load(f)
            
            await browser.close()
            logging.info("[PBLOGIN] Login successful for %s", username)
            return (True, "✅ Login successful with sessionid!", storage_state)
            
    except Exception as e:
        logging.error("[PBLOGIN] Exception: %s", e)
        return (False, f"❌ Error: {str(e)}", None)

USERNAME, PASSWORD = range(2)
PLO_USERNAME, PLO_PASSWORD = range(2)
PBLOGIN_USERNAME, PBLOGIN_SESSIONID = range(2)
PBATTACK_LINK, PBATTACK_MESSAGES = range(2)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Welcome to Spyther's bot ⚡ Type /help")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_authorized(user_id):
        await update.message.reply_text("⚠️ Not authorized ⚠️")
        return
    help_text = """
🌟 COMMANDS 🌟
📱 /login - PTY login
🔐 /plogin - Async Playwright login
👀 /viewmyac - View accounts
🔄 /setig <#> - Set default
📦 /pair <ig1-ig2> - Create pair
✨ /unpair - Del pair
⏱️ /switch <min> - Switch interval
🔢 /threads <1-5> - Threads
⚙️ /viewpref - Preferences
💥 /attack - Start attack
🛑 /stop <pid/all> - Stop
📋 /task - View tasks
🚪 /logout <user> - Logout
🧹 /kill - Kill PTY
📊 /usg - Usage
    """
    if is_owner(user_id):
        help_text += """
👑 ADMIN 👑
➕ /add <tg_id>
➖ /remove <tg_id>
📜 /users
🧹 /flush
        """
    await update.message.reply_text(help_text)

# ===== LOGIN HANDLERS =====
async def login_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    if not is_authorized(user_id):
        await update.message.reply_text("⚠️ Not authorized ⚠️")
        return ConversationHandler.END
    await update.message.reply_text("📱 Username:")
    return USERNAME

async def get_username(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['ig_username'] = update.message.text.strip().lower()
    await update.message.reply_text("🔒 Password:")
    return PASSWORD

async def get_password(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    username = context.user_data['ig_username']
    password = update.message.text.strip()
    
    with SESSIONS_LOCK:
        if user_id in SESSIONS:
            await update.message.reply_text("⚠️ Session running. /kill first")
            return ConversationHandler.END

    pid, master_fd = pty.fork()
    if pid == 0:
        try:
            child_playwright_login(user_id, username, password)
        except SystemExit:
            os._exit(0)
        except Exception as e:
            print(f"[CHILD] Error: {e}")
            os._exit(1)
    else:
        t = threading.Thread(target=reader_thread, args=(user_id, chat_id, master_fd, username, password), daemon=True)
        t.start()
        with SESSIONS_LOCK:
            SESSIONS[user_id] = {"pid": pid, "master_fd": master_fd, "thread": t, "username": username}
    
    return ConversationHandler.END

# ===== PLOGIN HANDLERS =====
async def plogin_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    if not is_authorized(user_id):
        await update.message.reply_text("⚠️ Not authorized ⚠️")
        return ConversationHandler.END
    await update.message.reply_text("🔐 Username:")
    return PLO_USERNAME

async def plogin_get_username(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['pl_username'] = update.message.text.strip().lower()
    await update.message.reply_text("🔒 Password:")
    return PLO_PASSWORD

async def plogin_get_password(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    username = context.user_data['pl_username']
    password = update.message.text.strip()

    await update.message.reply_text("🔄 Starting async login...")

    try:
        state_file = await playwright_login_and_save_state(username, password, user_id)
        state = json.load(open(state_file))

        if user_id not in users_data:
            users_data[user_id] = {
                'accounts': [],
                'default': None,
                'pairs': None,
                'switch_minutes': 10,
                'threads': 1,
            }
            save_user_data(user_id, users_data[user_id])

        data = users_data[user_id]
        found = False
        for i, acc in enumerate(data['accounts']):
            if acc.get('ig_username', '').strip().lower() == username:
                acc['password'] = password
                acc['storage_state'] = state
                data['default'] = i
                found = True
                break

        if not found:
            data['accounts'].append({
                'ig_username': username,
                'password': password,
                'storage_state': state,
            })
            data['default'] = len(data['accounts']) - 1

        save_user_data(user_id, data)
        await update.message.reply_text("✅ Login successful! 🎉")

    except ValueError as ve:
        await update.message.reply_text(f"❌ {str(ve)}")
    except Exception as e:
        logging.exception("PLOGIN error")
        await update.message.reply_text(f"❌ Error: {e}")

    return ConversationHandler.END

# ===== COOKIE-BASED LOGIN HANDLERS (sessionid) =====
async def pblogin_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start cookie-based login - ask for username."""
    user_id = update.effective_user.id
    if not is_authorized(user_id):
        await update.message.reply_text("⚠️ Not authorized ⚠️")
        return ConversationHandler.END
    await update.message.reply_text(
        "🍪 Cookie-Based Login\n\n"
        "Enter your Instagram username:"
    )
    return PBLOGIN_USERNAME

async def pblogin_get_username(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Get username and ask for sessionid."""
    context.user_data['pb_username'] = update.message.text.strip().lower()
    await update.message.reply_text(
        "📋 Enter your sessionid cookie value:\n\n"
        "Find it in your browser:\n"
        "1. Open instagram.com\n"
        "2. Dev Tools (F12) → Application → Cookies\n"
        "3. Copy 'sessionid' value"
    )
    return PBLOGIN_SESSIONID

async def pblogin_get_sessionid(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Get sessionid and perform login in background thread."""
    user_id = update.effective_user.id
    username = context.user_data['pb_username']
    sessionid = update.message.text.strip()

    if not sessionid or len(sessionid) < 10:
        await update.message.reply_text("❌ Invalid sessionid format")
        return PBLOGIN_SESSIONID

    msg = await update.message.reply_text("🔄 Logging in with sessionid... (may take 10-20 seconds)")

    def run_login():
        async def login_async():
            success, message, storage_state = await playwright_cookie_login(sessionid, user_id, username)
            
            # Store result in context
            context.user_data['pb_result'] = {
                'success': success,
                'message': message,
                'storage_state': storage_state,
                'username': username,
            }
            
            try:
                # Update status message
                await msg.edit_text(message)
                
                if success:
                    # Store account if successful
                    if user_id not in users_data:
                        users_data[user_id] = {
                            'accounts': [],
                            'default': None,
                            'pairs': None,
                            'switch_minutes': 10,
                            'threads': 1,
                        }
                    
                    data = users_data[user_id]
                    found = False
                    for i, acc in enumerate(data['accounts']):
                        if acc.get('ig_username', '').strip().lower() == username:
                            acc['storage_state'] = storage_state
                            data['default'] = i
                            found = True
                            break
                    
                    if not found:
                        data['accounts'].append({
                            'ig_username': username,
                            'password': '[cookie-based]',
                            'storage_state': storage_state,
                        })
                        data['default'] = len(data['accounts']) - 1
                    
                    save_user_data(user_id, data)
                    await update.message.reply_text("✅ Account stored! Ready to use.")
                    logging.info(f"[PBLOGIN] Successful login for {username} (user_id={user_id})")
                    
            except Exception as e:
                logging.error(f"[PBLOGIN] Error updating message: {e}")

        # Run async code in event loop
        asyncio.run_coroutine_threadsafe(login_async(), LOOP)

    # Run in thread to avoid blocking
    thread = threading.Thread(target=run_login, daemon=True)
    thread.start()

    return ConversationHandler.END

# ===== PBATTACK: SAFE MESSAGE SENDER HANDLERS =====
async def pbattack_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start safe message sender - ask for group chat link."""
    user_id = update.effective_user.id
    if not is_authorized(user_id):
        await update.message.reply_text("⚠️ Not authorized ⚠️")
        return ConversationHandler.END
    
    if user_id not in users_data or not users_data[user_id]['accounts']:
        await update.message.reply_text("❌ No account. /login or /pblogin first")
        return ConversationHandler.END
    
    await update.message.reply_text(
        "🔗 Enter group chat link:\n\n"
        "Example: https://www.instagram.com/direct/t/123456789/"
    )
    return PBATTACK_LINK

async def pbattack_get_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Get group chat link and ask for messages."""
    chat_link = update.message.text.strip()
    
    # Validate Instagram link
    if not ("instagram.com" in chat_link and ("direct" in chat_link or "messages" in chat_link)):
        await update.message.reply_text(
            "❌ Invalid link. Must be Instagram DM/group chat:\n"
            "https://www.instagram.com/direct/..."
        )
        return PBATTACK_LINK
    
    context.user_data['pb_chat_link'] = chat_link
    await update.message.reply_text(
        "📝 Send messages:\n"
        "- Multiple lines as separate messages\n"
        "- Or upload .txt file\n\n"
        "Each line = 1 message"
    )
    return PBATTACK_MESSAGES

async def pbattack_get_messages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Get messages and start sending with multi-worker system."""
    user_id = update.effective_user.id
    chat_link = context.user_data['pb_chat_link']
    
    # Get messages from file or text
    randomid = str(uuid.uuid4())[:8]
    messages_file = f"{user_id}_{randomid}_pbattack.txt"
    
    try:
        # Check if document uploaded
        if update.message.document:
            document = update.message.document
            file = await document.get_file()
            await file.download_to_drive(messages_file)
            logging.info("[PBATTACK] Downloaded file: %s", messages_file)
        else:
            # Use text input
            raw_text = (update.message.text or "").strip()
            if not raw_text:
                await update.message.reply_text("❌ No messages provided")
                return PBATTACK_MESSAGES
            
            text = unicodedata.normalize("NFKC", raw_text)
            with open(messages_file, 'w', encoding='utf-8') as f:
                f.write(text)
            logging.info("[PBATTACK] Created message file: %s", messages_file)
    
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")
        return ConversationHandler.END
    
    # Read messages from file
    try:
        with open(messages_file, 'r', encoding='utf-8') as f:
            messages = [line.strip() for line in f.readlines() if line.strip()]
        
        if not messages:
            await update.message.reply_text("❌ No messages in file")
            return ConversationHandler.END
    except Exception as e:
        await update.message.reply_text(f"❌ Error reading messages: {e}")
        return ConversationHandler.END
    
    # Get account with storage state
    data = users_data[user_id]
    acc = data['accounts'][data['default']]
    storage_state = acc.get('storage_state', {})
    
    # Show starting message
    status_msg = await update.message.reply_text(
        f"⚡ Starting multi-worker sender...\n"
        f"📊 Total messages: {len(messages)}\n"
        f"👷 Workers: 3\n"
        f"🔄 Status: Initializing..."
    )
    
    def run_attack():
        async def attack_async():
            # Run coordinator
            success, result_msg, stats = await playwright_pbattack_coordinator(
                chat_link=chat_link,
                messages=messages,
                user_id=user_id,
                storage_state=storage_state,
                num_workers=2  # 2 concurrent workers - ULTRA FAST
            )
            
            try:
                # Update final message with stats
                final_msg = result_msg + f"\n\n📈 Final Stats:\n"
                final_msg += f"✅ Sent: {stats['sent']}\n"
                final_msg += f"❌ Failed: {stats['failed']}\n"
                final_msg += f"📊 Total: {stats['total']}\n"
                final_msg += f"⏹️  /stoppb to stop (if still running)"
                
                await status_msg.edit_text(final_msg)
                logging.info(f"[PBATTACK] User {user_id}: {result_msg}")
            except Exception as e:
                logging.error(f"[PBATTACK] Error updating message: {e}")
            
            # Clean up message file
            try:
                if os.path.exists(messages_file):
                    os.remove(messages_file)
                    logging.info("[PBATTACK] Cleaned up message file")
            except:
                pass
        
        asyncio.run_coroutine_threadsafe(attack_async(), LOOP)
    
    # Run in thread (non-blocking)
    thread = threading.Thread(target=run_attack, daemon=True)
    thread.start()
    
    return ConversationHandler.END

# ===== STOP COMMAND FOR PBATTACK =====
async def pbattack_stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Stop the current pbattack message sending."""
    user_id = update.effective_user.id
    
    if user_id in pbattack_stop_events and pbattack_active.get(user_id, False):
        # Signal all workers to stop
        pbattack_stop_events[user_id].set()
        await update.message.reply_text("⏹️ Stop signal sent to all workers...\nWaiting for graceful shutdown...")
        logging.info("[PBATTACK] Stop requested by user %s", user_id)
    else:
        await update.message.reply_text("⚠️ No active pbattack running")

# ===== ACCOUNT MANAGEMENT =====
async def viewmyac(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_authorized(user_id):
        await update.message.reply_text("⚠️ Not authorized ⚠️")
        return
    if user_id not in users_data:
        await update.message.reply_text("❌ No accounts. /login first")
        return
    data = users_data[user_id]
    msg = "👀 Your accounts:\n"
    for i, acc in enumerate(data['accounts']):
        default = " ⭐" if data['default'] == i else ""
        msg += f"{i+1}. {acc['ig_username']}{default}\n"
    await update.message.reply_text(msg)

async def setig(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_authorized(user_id):
        await update.message.reply_text("⚠️ Not authorized ⚠️")
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("❗ /setig <number>")
        return
    num = int(context.args[0]) - 1
    if user_id not in users_data:
        await update.message.reply_text("❌ No accounts")
        return
    data = users_data[user_id]
    if num < 0 or num >= len(data['accounts']):
        await update.message.reply_text("⚠️ Invalid")
        return
    data['default'] = num
    save_user_data(user_id, data)
    await update.message.reply_text(f"✅ {data['accounts'][num]['ig_username']} is default")

async def logout_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_authorized(user_id):
        await update.message.reply_text("⚠️ Not authorized ⚠️")
        return
    if not context.args:
        await update.message.reply_text("❗ /logout <username>")
        return
    username = context.args[0].strip()
    if user_id not in users_data:
        await update.message.reply_text("❌ No accounts")
        return
    data = users_data[user_id]
    for i, acc in enumerate(data['accounts']):
        if acc['ig_username'] == username:
            del data['accounts'][i]
            if data['default'] == i:
                data['default'] = 0 if data['accounts'] else None
            elif data['default'] > i:
                data['default'] -= 1
            if data['pairs']:
                pl = data['pairs']['list']
                if username in pl:
                    pl.remove(username)
                    if not pl:
                        data['pairs'] = None
            break
    else:
        await update.message.reply_text("⚠️ Not found")
        return
    save_user_data(user_id, data)
    for f in [f"sessions/{user_id}_{username}_state.json", f"sessions/{user_id}_{username}_session.json"]:
        if os.path.exists(f):
            os.remove(f)
    await update.message.reply_text(f"✅ Logged out {username}")

async def pair_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_authorized(user_id):
        await update.message.reply_text("⚠️ Not authorized ⚠️")
        return
    if not context.args:
        await update.message.reply_text("❗ /pair ig1-ig2-ig3")
        return
    us = [u.strip() for u in '-'.join(context.args).split('-') if u.strip()]
    if len(us) < 2:
        await update.message.reply_text("❗ Need 2+ accounts")
        return
    if user_id not in users_data:
        await update.message.reply_text("❌ /login first")
        return
    data = users_data[user_id]
    accounts_set = {acc['ig_username'] for acc in data['accounts']}
    missing = [u for u in us if u not in accounts_set]
    if missing:
        await update.message.reply_text(f"⚠️ Not found: {missing[0]}")
        return
    data['pairs'] = {'list': us, 'default_index': 0}
    for i, acc in enumerate(data['accounts']):
        if acc['ig_username'] == us[0]:
            data['default'] = i
            break
    save_user_data(user_id, data)
    await update.message.reply_text(f"✅ Pair: {len(us)} accounts")

async def unpair_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_authorized(user_id):
        await update.message.reply_text("⚠️ Not authorized ⚠️")
        return
    if user_id not in users_data or not users_data[user_id].get('pairs'):
        await update.message.reply_text("❌ No pair")
        return
    data = users_data[user_id]
    if context.args and context.args[0].lower() == "all":
        data['pairs'] = None
        save_user_data(user_id, data)
        await update.message.reply_text("🧹 Pair removed")
        return
    await update.message.reply_text("Pair removed" if not context.args else f"Usage: /unpair or /unpair all")

async def switch_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_authorized(user_id):
        await update.message.reply_text("⚠️ Not authorized ⚠️")
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("❗ /switch <minutes>")
        return
    min_ = int(context.args[0])
    if user_id not in users_data:
        await update.message.reply_text("❌ /login first")
        return
    data = users_data[user_id]
    if not data.get('pairs'):
        await update.message.reply_text("⚠️ /pair first")
        return
    if min_ < 5:
        await update.message.reply_text("⚠️ Min 5 min")
        return
    data['switch_minutes'] = min_
    save_user_data(user_id, data)
    await update.message.reply_text(f"⏱️ {min_} min")

async def threads_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_authorized(user_id):
        await update.message.reply_text("⚠️ Not authorized ⚠️")
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("❗ /threads <1-5>")
        return
    n = int(context.args[0])
    if n < 1 or n > 5:
        await update.message.reply_text("⚠️ 1-5 only")
        return
    if user_id not in users_data:
        users_data[user_id] = {'accounts': [], 'default': None, 'pairs': None, 'switch_minutes': 10, 'threads': 1}
    data = users_data[user_id]
    data['threads'] = n
    save_user_data(user_id, data)
    await update.message.reply_text(f"🔁 {n}")

async def viewpref(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_authorized(user_id):
        await update.message.reply_text("⚠️ Not authorized ⚠️")
        return
    if user_id not in users_data:
        await update.message.reply_text("❌ No data")
        return
    data = users_data[user_id]
    accounts = ', '.join([acc['ig_username'] for acc in data['accounts']])
    msg = f"Switch: {data.get('switch_minutes', 10)}m\nThreads: {data.get('threads', 1)}\nAccounts: {accounts}"
    await update.message.reply_text(msg)

# ===== ATTACK MODE =====
MODE, SELECT_GC, TARGET, MESSAGES = range(4)

async def attack_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    if not is_authorized(user_id):
        await update.message.reply_text("⚠️ Not authorized ⚠️")
        return ConversationHandler.END
    if user_id not in users_data or not users_data[user_id]['accounts']:
        await update.message.reply_text("❗ /login first")
        return ConversationHandler.END
    data = users_data[user_id]
    if data['default'] is None:
        data['default'] = 0
        save_user_data(user_id, data)
    await update.message.reply_text("dm or gc?")
    return MODE

async def get_mode(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    text = update.message.text.lower().strip()
    data = users_data[user_id]

    if 'dm' in text:
        context.user_data['mode'] = 'dm'
        await update.message.reply_text("Username:")
        return TARGET

    elif 'gc' in text:
        acc = data['accounts'][data['default']]
        fetch_msg = await update.message.reply_text("🔍 Loading...")

        user_fetching.add(user_id)
        try:
            # Run async function in thread
            loop = asyncio.get_event_loop()
            groups = await loop.run_in_executor(
                None,
                lambda: asyncio.run(list_group_chats_playwright(acc['ig_username'], acc['storage_state'], 10))
            )
        except:
            groups = []
        finally:
            user_fetching.discard(user_id)
            try:
                await fetch_msg.delete()
            except:
                pass

        if user_id in user_cancel_fetch:
            user_cancel_fetch.discard(user_id)
            await update.message.reply_text("❌ Cancelled")
            return ConversationHandler.END

        context.user_data['groups'] = groups

        if not groups:
            await update.message.reply_text("❌ No groups")
            return ConversationHandler.END

        msg = "Select:\n"
        for i, g in enumerate(groups):
            msg += f"{i+1}. {g.get('display', '?')[:30]}\n"
        await update.message.reply_text(msg)
        return SELECT_GC

    else:
        await update.message.reply_text("dm or gc?")
        return MODE

async def select_gc_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    text = update.message.text.strip()
    try:
        num = int(text) - 1
        groups = context.user_data.get('groups', [])
        if 0 <= num < len(groups):
            g = groups[num]
            context.user_data['thread_url'] = g['url']
            context.user_data['target_display'] = g.get('display', 'Unknown')
            await update.message.reply_text("Messages (msg1 & msg2) or .txt file")
            return MESSAGES
        else:
            await update.message.reply_text(f"1-{len(groups)}")
            return SELECT_GC
    except ValueError:
        await update.message.reply_text("Number only")
        return SELECT_GC

async def get_target_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    target_u = update.message.text.strip().lstrip('@')
    if not target_u:
        await update.message.reply_text("Invalid")
        return TARGET
    context.user_data['target_display'] = target_u
    data = users_data[user_id]
    acc = data['accounts'][data['default']]
    
    # Run async function
    loop = asyncio.get_event_loop()
    thread_url = await loop.run_in_executor(
        None,
        lambda: asyncio.run(get_dm_thread_url_playwright(acc['ig_username'], acc['storage_state'], target_u))
    )
    
    if not thread_url:
        await update.message.reply_text("❌ No DM found")
        return ConversationHandler.END
    
    context.user_data['thread_url'] = thread_url
    await update.message.reply_text("Messages (msg1 & msg2) or .txt file")
    return MESSAGES

async def get_messages_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    document = update.message.document

    if not document:
        await update.message.reply_text("Upload .txt file")
        return ConversationHandler.END

    file = await document.get_file()
    randomid = str(uuid.uuid4())[:8]
    names_file = f"{user_id}_{randomid}.txt"

    await file.download_to_drive(names_file)
    context.user_data['uploaded_names_file'] = names_file

    return await get_messages(update, context)

async def get_messages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id

    uploaded_file = context.user_data.pop('uploaded_names_file', None)

    if uploaded_file and os.path.exists(uploaded_file):
        names_file = uploaded_file
    else:
        raw_text = (update.message.text or "").strip()
        text = unicodedata.normalize("NFKC", raw_text)
        randomid = str(uuid.uuid4())[:8]
        names_file = f"{user_id}_{randomid}.txt"
        try:
            with open(names_file, 'w', encoding='utf-8') as f:
                f.write(text)
        except Exception as e:
            await update.message.reply_text(f"Error: {e}")
            return ConversationHandler.END

    data = users_data[user_id]
    pairs = data.get('pairs')
    pair_list = pairs['list'] if pairs else [data['accounts'][data['default']]['ig_username']]
    
    warning = "⚠️ Single account = ban risk. /pair\n\n" if len(pair_list) == 1 else ""
    
    switch_minutes = data.get('switch_minutes', 10)
    threads_n = data.get('threads', 1)
    tasks = users_tasks.get(user_id, [])
    running = [t for t in tasks if t.get('type') == 'message_attack' and t['status'] == 'running' and t['proc'].poll() is None]
    
    if len(running) >= 5:
        await update.message.reply_text("⚠️ Max 5 running")
        if os.path.exists(names_file):
            os.remove(names_file)
        return ConversationHandler.END

    thread_url = context.user_data['thread_url']
    target_display = context.user_data['target_display']
    target_mode = context.user_data['mode']
    start_idx = pairs['default_index'] if pairs else 0
    start_u = pair_list[start_idx]
    start_acc = next(acc for acc in data['accounts'] if acc['ig_username'] == start_u)
    start_pass = start_acc['password']
    start_u = start_u.strip().lower()
    state_file = f"sessions/{user_id}_{start_u}_state.json"
    
    if not os.path.exists(state_file):
        with open(state_file, 'w') as f:
            json.dump(start_acc['storage_state'], f)

    cmd = [
        "python3", "msg.py",
        "--username", start_u,
        "--password", start_pass,
        "--thread-url", thread_url,
        "--names", names_file,
        "--tabs", str(threads_n),
        "--headless", "true",
        "--storage-state", state_file
    ]
    
    proc = subprocess.Popen(cmd)
    running_processes[proc.pid] = proc
    pid = proc.pid
    task_id = str(uuid.uuid4())
    task = {
        "id": task_id,
        "user_id": user_id,
        "type": "message_attack",
        "pair_list": pair_list,
        "pair_index": start_idx,
        "switch_minutes": switch_minutes,
        "threads": threads_n,
        "names_file": names_file,
        "target_thread_url": thread_url,
        "target_type": target_mode,
        "target_display": target_display,
        "last_switch_time": time.time(),
        "status": "running",
        "cmd": cmd,
        "pid": pid,
        "display_pid": pid,
        "proc_list": [pid],
        "proc": proc,
        "start_time": time.time()
    }
    persistent_tasks.append(task)
    save_persistent_tasks()
    tasks.append(task)
    users_tasks[user_id] = tasks
    logging.info(f"Attack start user={user_id} target={target_display} pid={pid}")

    status = "Spamming!\n"
    curr_u = pair_list[task['pair_index']]
    for u in pair_list:
        if u == curr_u:
            status += f"using - {u}\n"
        else:
            status += f"cooldown - {u}\n"
    status += f"Stop: /stop {task['display_pid']}"

    sent_msg = await update.message.reply_text(warning + status)
    task['status_chat_id'] = update.message.chat_id
    task['status_msg_id'] = sent_msg.message_id
    return ConversationHandler.END

# ===== TASK PERSISTENCE =====
def load_persistent_tasks():
    global persistent_tasks
    if os.path.exists(TASKS_FILE):
        with open(TASKS_FILE, 'r') as f:
            persistent_tasks = json.load(f)
    else:
        persistent_tasks = []

def save_persistent_tasks():
    safe_list = []
    for t in persistent_tasks:
        cleaned = {}
        for k, v in t.items():
            if k == 'proc':
                continue
            if isinstance(v, (int, float, str, bool, dict, list, type(None))):
                cleaned[k] = v
            else:
                try:
                    json.dumps(v)
                    cleaned[k] = v
                except:
                    cleaned[k] = str(v)
        safe_list.append(cleaned)
    temp_file = TASKS_FILE + '.tmp'
    with open(temp_file, 'w') as f:
        json.dump(safe_list, f, indent=2)
    os.replace(temp_file, TASKS_FILE)

def mark_task_stopped_persistent(task_id: str):
    global persistent_tasks
    for task in persistent_tasks:
        if task['id'] == task_id:
            task['status'] = 'stopped'
            save_persistent_tasks()
            break

def update_task_pid_persistent(task_id: str, new_pid: int):
    global persistent_tasks
    for task in persistent_tasks:
        if task['id'] == task_id:
            task['pid'] = new_pid
            save_persistent_tasks()
            break

def mark_task_completed_persistent(task_id: str):
    global persistent_tasks
    for task in persistent_tasks:
        if task['id'] == task_id:
            task['status'] = 'completed'
            save_persistent_tasks()
            break

def restore_tasks_on_start():
    load_persistent_tasks()
    print(f"🔄 Restoring running tasks...")
    for task in persistent_tasks[:]:
        if task.get('type') == 'message_attack' and task['status'] == 'running':
            old_pid = task['pid']
            try:
                os.kill(old_pid, signal.SIGTERM)
                time.sleep(1)
            except OSError:
                pass
            user_id = task['user_id']
            data = users_data.get(user_id)
            if not data:
                mark_task_stopped_persistent(task['id'])
                continue
            pair_list = task['pair_list']
            curr_idx = task['pair_index']
            curr_u = pair_list[curr_idx]
            curr_acc = None
            for acc in data['accounts']:
                if acc['ig_username'] == curr_u:
                    curr_acc = acc
                    break
            if not curr_acc:
                mark_task_stopped_persistent(task['id'])
                continue
            curr_pass = curr_acc['password']
            curr_u = curr_u.strip().lower()
            state_file = f"sessions/{user_id}_{curr_u}_state.json"
            if not os.path.exists(state_file):
                with open(state_file, 'w') as f:
                    json.dump(curr_acc['storage_state'], f)
            names_file = task['names_file']
            if not os.path.exists(names_file):
                mark_task_stopped_persistent(task['id'])
                continue
            cmd = [
                "python3", "msg.py",
                "--username", curr_u,
                "--password", curr_pass,
                "--thread-url", task['target_thread_url'],
                "--names", names_file,
                "--tabs", str(task['threads']),
                "--headless", "true",
                "--storage-state", state_file
            ]
            try:
                proc = subprocess.Popen(cmd)
                running_processes[proc.pid] = proc
                new_pid = proc.pid
                update_task_pid_persistent(task['id'], new_pid)
                mem_task = task.copy()
                mem_task['proc'] = proc
                mem_task['proc_list'] = [proc.pid]
                if user_id not in users_tasks:
                    users_tasks[user_id] = []
                users_tasks[user_id].append(mem_task)
                print(f"✅ Restored {task['id']} | PID: {new_pid}")
            except Exception as e:
                logging.error(f"Failed to restore {task['id']}: {e}")
                mark_task_stopped_persistent(task['id'])
    save_persistent_tasks()

def get_switch_update(task: Dict) -> str:
    pair_list = task['pair_list']
    curr_idx = task['pair_index']
    curr_u = pair_list[curr_idx]
    lines = []
    for u in pair_list:
        if u == curr_u:
            lines.append(f"using - {u}")
        else:
            lines.append(f"cooldown - {u}")
    return '\n'.join(lines)

def switch_task_sync(task: Dict):
    user_id = task['user_id']
    try:
        old_proc = task.get('proc')
        old_pid = task.get('pid')
    except:
        old_proc = None
        old_pid = task.get('pid')

    task['pair_index'] = (task['pair_index'] + 1) % len(task['pair_list'])
    next_u = task['pair_list'][task['pair_index']]
    data = users_data.get(user_id)
    if not data:
        logging.error(f"No data for user {user_id}")
        return

    next_acc = next((a for a in data['accounts'] if a['ig_username'] == next_u), None)
    if not next_acc:
        logging.error(f"Account {next_u} not found")
        return

    next_pass = next_acc['password']
    next_state_file = f"sessions/{user_id}_{next_u}_state.json"
    if not os.path.exists(next_state_file):
        try:
            with open(next_state_file, 'w') as f:
                json.dump(next_acc.get('storage_state', {}), f)
        except Exception as e:
            logging.error(f"Failed to write state: {e}")

    new_cmd = [
        "python3", "msg.py",
        "--username", next_u,
        "--password", next_pass,
        "--thread-url", task['target_thread_url'],
        "--names", task['names_file'],
        "--tabs", str(task['threads']),
        "--headless", "true",
        "--storage-state", next_state_file
    ]
    try:
        new_proc = subprocess.Popen(new_cmd)
    except Exception as e:
        logging.error(f"Failed to launch new proc: {e}")
        return

    task['proc_list'].append(new_proc.pid)
    running_processes[new_proc.pid] = new_proc
    task['cmd'] = new_cmd
    task['pid'] = new_proc.pid
    task['proc'] = new_proc
    task['last_switch_time'] = time.time()
    try:
        update_task_pid_persistent(task['id'], task['pid'])
    except Exception as e:
        logging.error(f"Failed to update persistent pid: {e}")

    if old_proc and old_pid != new_proc.pid:
        try:
            time.sleep(5)
            try:
                old_proc.terminate()
            except:
                pass
            time.sleep(2)
            if old_proc.poll() is None:
                try:
                    old_proc.kill()
                except:
                    pass
            if old_pid in task['proc_list']:
                task['proc_list'].remove(old_pid)
            if old_pid in running_processes:
                running_processes.pop(old_pid, None)
        except Exception as e:
            logging.error(f"Error stopping old proc: {e}")

    try:
        chat_id = task.get('status_chat_id', user_id)
        msg_id = task.get('status_msg_id')
        text = "Spamming!\n" + get_switch_update(task)
        if msg_id:
            asyncio.run_coroutine_threadsafe(
                APP.bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text),
                LOOP
            )
    except Exception as e:
        logging.error(f"Failed to update status: {e}")

def switch_monitor():
    while True:
        time.sleep(30)
        for user_id in list(users_tasks):
            if user_id not in users_tasks:
                continue
            for task in users_tasks[user_id]:
                if task.get('type') == 'message_attack' and task['status'] == 'running' and task['proc'].poll() is None:
                    due_time = task['last_switch_time'] + task['switch_minutes'] * 60
                    if time.time() >= due_time:
                        if len(task['pair_list']) > 1:
                            switch_task_sync(task)

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_authorized(user_id):
        await update.message.reply_text("⚠️ Not authorized ⚠️")
        return
    if not context.args:
        await update.message.reply_text("❗ /stop <pid> or /stop all")
        return
    arg = context.args[0]
    if user_id not in users_tasks or not users_tasks[user_id]:
        await update.message.reply_text("❌ No tasks")
        return
    tasks = users_tasks[user_id]
    if arg == 'all':
        stopped = 0
        for task in tasks[:]:
            proc = task['proc']
            proc.terminate()
            await asyncio.sleep(3)
            if proc.poll() is None:
                proc.kill()
            if 'names_file' in task and os.path.exists(task['names_file']):
                os.remove(task['names_file'])
            mark_task_stopped_persistent(task['id'])
            tasks.remove(task)
            stopped += 1
        await update.message.reply_text(f"🛑 Stopped {stopped}")
    elif arg.isdigit():
        pid_to_stop = int(arg)
        for task in tasks[:]:
            if task.get('display_pid') == pid_to_stop:
                for backend_pid in task.get('proc_list', []):
                    backend_proc = running_processes.get(backend_pid)
                    if backend_proc:
                        try:
                            backend_proc.terminate()
                        except:
                            pass
                        await asyncio.sleep(3)
                        if backend_proc.poll() is None:
                            try:
                                backend_proc.kill()
                            except:
                                pass
                    running_processes.pop(backend_pid, None)
                mark_task_stopped_persistent(task['id'])
                if 'names_file' in task and os.path.exists(task['names_file']):
                    os.remove(task['names_file'])
                tasks.remove(task)
                await update.message.reply_text(f"🛑 Stopped {pid_to_stop}")
                break
        else:
            await update.message.reply_text("⚠️ Not found")
    users_tasks[user_id] = tasks

async def task_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_authorized(user_id):
        await update.message.reply_text("⚠️ Not authorized ⚠️")
        return
    if user_id not in users_tasks or not users_tasks[user_id]:
        await update.message.reply_text("❌ No tasks")
        return
    tasks = users_tasks[user_id]
    active = []
    for t in tasks:
        if t['proc'].poll() is None:
            active.append(t)
        else:
            mark_task_completed_persistent(t['id'])
    users_tasks[user_id] = active
    if not active:
        await update.message.reply_text("❌ No active")
        return
    msg = "📋 Active:\n"
    for task in active:
        preview = task['target_display'][:20]
        pid = task.get('display_pid', task['pid'])
        msg += f"{pid} - {preview}\n"
    await update.message.reply_text(msg)

async def usg_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorized(update.effective_user.id):
        await update.message.reply_text("⚠️ Not authorized ⚠️")
        return
    cpu = psutil.cpu_percent(interval=1)
    mem = psutil.virtual_memory()
    ram_used = mem.used / (1024 ** 3)
    ram_total = mem.total / (1024 ** 3)
    msg = f"🖥️ CPU: {cpu:.1f}%\n💾 RAM: {ram_used:.1f}/{ram_total:.1f}GB"
    await update.message.reply_text(msg)

async def cancel_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if user_id in user_fetching:
        user_fetching.discard(user_id)
        user_cancel_fetch.add(user_id)
        await update.message.reply_text("❌ Cancelled")

async def add_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_owner(user_id):
        await update.message.reply_text("⚠️ Admin only ⚠️")
        return
    if len(context.args) != 1:
        await update.message.reply_text("❗ /add <tg_id>")
        return
    try:
        tg_id = int(context.args[0])
        if any(u['id'] == tg_id for u in authorized_users):
            await update.message.reply_text("Already added")
            return
        authorized_users.append({'id': tg_id, 'username': ''})
        save_authorized()
        await update.message.reply_text(f"✅ Added {tg_id}")
    except:
        await update.message.reply_text("❌ Invalid")

async def remove_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_owner(user_id):
        await update.message.reply_text("⚠️ Admin only ⚠️")
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("❗ /remove <tg_id>")
        return
    tg_id = int(context.args[0])
    global authorized_users
    authorized_users = [u for u in authorized_users if u['id'] != tg_id]
    save_authorized()
    await update.message.reply_text(f"✅ Removed {tg_id}")

async def list_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_owner(user_id):
        await update.message.reply_text("⚠️ Admin only ⚠️")
        return
    if not authorized_users:
        await update.message.reply_text("No users")
        return
    msg = "📜 Users:\n"
    for i, u in enumerate(authorized_users, 1):
        msg += f"{i}. {u['id']}\n"
    await update.message.reply_text(msg)

def main_bot():
    from telegram.request import HTTPXRequest
    request = HTTPXRequest(connect_timeout=30, read_timeout=30, write_timeout=30)
    application = Application.builder().token(BOT_TOKEN).request(request).build()
    global APP, LOOP
    APP = application
    LOOP = asyncio.get_event_loop()
    
    restore_tasks_on_start()
    
    monitor_thread = threading.Thread(target=switch_monitor, daemon=True)
    monitor_thread.start()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("viewmyac", viewmyac))
    application.add_handler(CommandHandler("setig", setig))
    application.add_handler(CommandHandler("pair", pair_command))
    application.add_handler(CommandHandler("unpair", unpair_command))
    application.add_handler(CommandHandler("switch", switch_command))
    application.add_handler(CommandHandler("threads", threads_command))
    application.add_handler(CommandHandler("viewpref", viewpref))
    application.add_handler(CommandHandler("stop", stop))
    application.add_handler(CommandHandler("pbstop", pbattack_stop))
    application.add_handler(CommandHandler("task", task_command))
    application.add_handler(CommandHandler("add", add_user))
    application.add_handler(CommandHandler("remove", remove_user))
    application.add_handler(CommandHandler("users", list_users))
    application.add_handler(CommandHandler("logout", logout_command))
    application.add_handler(CommandHandler("kill", cmd_kill))
    application.add_handler(CommandHandler("flush", flush))
    application.add_handler(CommandHandler("usg", usg_command))
    application.add_handler(CommandHandler("cancel", cancel_handler))

    conv_login = ConversationHandler(
        entry_points=[CommandHandler("login", login_start)],
        states={
            USERNAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_username)],
            PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_password)],
        },
        fallbacks=[],
    )
    application.add_handler(conv_login)

    conv_plogin = ConversationHandler(
        entry_points=[CommandHandler("plogin", plogin_start)],
        states={
            PLO_USERNAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, plogin_get_username)],
            PLO_PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, plogin_get_password)],
        },
        fallbacks=[],
    )
    application.add_handler(conv_plogin)

    conv_pblogin = ConversationHandler(
        entry_points=[CommandHandler("pblogin", pblogin_start)],
        states={
            PBLOGIN_USERNAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, pblogin_get_username)],
            PBLOGIN_SESSIONID: [MessageHandler(filters.TEXT & ~filters.COMMAND, pblogin_get_sessionid)],
        },
        fallbacks=[],
    )
    application.add_handler(conv_pblogin)

    conv_pbattack = ConversationHandler(
        entry_points=[CommandHandler("pbattack", pbattack_start)],
        states={
            PBATTACK_LINK: [MessageHandler(filters.TEXT & ~filters.COMMAND, pbattack_get_link)],
            PBATTACK_MESSAGES: [
                MessageHandler(filters.Document.FileExtension("txt"), pbattack_get_messages),
                MessageHandler(filters.TEXT & ~filters.COMMAND, pbattack_get_messages),
            ],
        },
        fallbacks=[],
    )
    application.add_handler(conv_pbattack)

    conv_attack = ConversationHandler(
        entry_points=[CommandHandler("attack", attack_start)],
        states={
            MODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_mode)],
            SELECT_GC: [MessageHandler(filters.TEXT & ~filters.COMMAND, select_gc_handler)],
            TARGET: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_target_handler)],
            MESSAGES: [
                MessageHandler(filters.Document.FileExtension("txt"), get_messages_file),
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_messages),
            ],
        },
        fallbacks=[],
    )
    application.add_handler(conv_attack)

    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    print("🚀 Pure Playwright Instagram bot starting!")
    application.run_polling()

if __name__ == "__main__":
    main_bot()
