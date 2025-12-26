import aiogram
import pandas as pd

from src.salary_bonus.config.defaults import ADDITIONAL_WORK
from src.salary_bonus.logger import logging
from src.salary_bonus.worksheets.worksheets import connect_to_add_work_archive


async def process_additional_work_data(
    archive_points: dict[str, pd.DataFrame], engineers: list[str], tg_bot: aiogram.Bot
) -> dict[str, pd.DataFrame]:
    """
    Собирает данные по дополнительным проектам из архива расчетов
    и производит расчет баллов для проектировщиков.
    """
    results = {}

    add_work_data_df: pd.DataFrame = connect_to_add_work_archive()

    if add_work_data_df is None:
        await tg_bot.send_message(
            f'Ошибка: таблица "{ADDITIONAL_WORK}" не найдена.\n'
            f"Возможно название было сменено.",
        )
        return results
    elif isinstance(add_work_data_df, pd.DataFrame) and add_work_data_df.empty:
        logging.warning(f"Таблица '{ADDITIONAL_WORK}' пуста, расчет не будет произведен.")
        return results

    return results
