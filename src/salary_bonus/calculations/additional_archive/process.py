import aiogram
import pandas as pd

from src.salary_bonus.calculations.additional_archive.counting_points import (
    count_add_points,
)
from src.salary_bonus.config.defaults import ADDITIONAL_WORK
from src.salary_bonus.logger import logging
from src.salary_bonus.utils import get_add_work_data

# from src.salary_bonus.worksheets.worksheets import send_project_data_to_spreadsheet

# from src.salary_bonus.worksheets.worksheets import connect_to_add_work_archive


async def process_additional_work_data(
    archive_points: dict[str, pd.DataFrame], engineers: list[str], tg_bot: aiogram.Bot
) -> dict[str, pd.DataFrame]:
    """
    Собирает данные по дополнительным проектам из архива расчетов
    и производит расчет баллов для проектировщиков.
    """
    results = {}

    add_work_data_df: pd.DataFrame = get_add_work_data()

    if add_work_data_df is None:
        await tg_bot.send_message(
            f'Ошибка: таблица "{ADDITIONAL_WORK}" не найдена.\n'
            f"Возможно название было сменено.",
        )
        return results
    elif isinstance(add_work_data_df, pd.DataFrame) and add_work_data_df.empty:
        logging.warning(
            f"Таблица '{ADDITIONAL_WORK}' пуста, расчет доп. работ не будет произведен."
        )
        return results

    for engineer in engineers:
        logging.info(
            f"Начинается расчет баллов за доп. работы для проектировщика {engineer}."
        )

        engineer_projects = add_work_data_df.loc[
            add_work_data_df["Разработал"].str.contains(f"{engineer}")
        ].reset_index(drop=True)

        if engineer_projects.empty:
            logging.info(f"Нет доп. проектов у проектировщика {engineer}.")
            continue

        engineer_projects["Баллы"] = engineer_projects.apply(
            count_add_points, axis=1, args=(engineer_projects,)
        )

        # send_project_data_to_spreadsheet(engineer_projects, engineer)

    return results
