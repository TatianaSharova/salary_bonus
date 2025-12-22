import aiogram
from aiogram.exceptions import TelegramAPIError

from src.salary_bonus.config.environment import TELEGRAM_CHAT_ID, TELEGRAM_TOKEN
from src.salary_bonus.exceptions import TelegramSendMessageError

tg_bot = aiogram.Bot(token=TELEGRAM_TOKEN)


async def send_tg_message(bot: aiogram.Bot, message: str):
    """Присылает сообщение в телеграме."""
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=f"{message}")
    except TelegramAPIError as error:
        raise TelegramSendMessageError(error)


class TelegramNotifier:
    """
    Класс для отправки сообщений в Telegram.
    """

    def __init__(
        self,
        token: str = TELEGRAM_TOKEN,
        chat_id: int | str = TELEGRAM_CHAT_ID,
    ):
        self.chat_id = chat_id
        self.bot = aiogram.Bot(token=token)

    async def send_message(self, message: str) -> None:
        """Отправляет сообщение в Telegram."""
        try:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
            )
        except TelegramAPIError as error:
            raise TelegramSendMessageError(error)

    async def close(self) -> None:
        """Закрывает сессию бота."""
        await self.bot.session.close()
