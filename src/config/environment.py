import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent
env_path = os.path.join(BASE_DIR, ".env")
load_dotenv(dotenv_path=env_path)

# credentials
CREDS_PATH = os.path.join(BASE_DIR, "creds.json")

# worksheets
EMAILS = os.getenv("EMAILS")
ENDPOINT_ATTENDANCE_SHEET = os.getenv("ENDPOINT_ATTENDANCE_SHEET")

# telegram
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
