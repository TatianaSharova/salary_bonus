import aiogram
from aiogram.exceptions import TelegramAPIError

from config.environment import TELEGRAM_CHAT_ID, TELEGRAM_TOKEN
from exceptions import TelegramSendMessageError

tg_bot = aiogram.Bot(token=TELEGRAM_TOKEN)


async def send_tg_message(bot: aiogram.Bot, message: str):
    """Присылает сообщение в телеграме."""
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=f"{message}")
    except TelegramAPIError as error:
        raise TelegramSendMessageError(error)
