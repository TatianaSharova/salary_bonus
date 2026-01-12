import time

import aiogram
import pandas as pd

from src.salary_bonus.calculations.additional_archive.counting_points import (
    count_add_points,
)
from src.salary_bonus.calculations.mounth_points import (
    calculate_by_month,
    empty_months_df,
)
from src.salary_bonus.config.defaults import ADDITIONAL_WORK, AFTER_ENG_SLEEP
from src.salary_bonus.logger import logging
from src.salary_bonus.utils import get_add_work_data
from src.salary_bonus.worksheets.worksheets import send_add_work_data_to_spreadsheet


async def process_additional_work_data(
    engineers: list[str],
    tg_bot: aiogram.Bot,
    archive_points: dict[str, pd.DataFrame] = None,
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
            f'Таблица "{ADDITIONAL_WORK}" пуста, расчет доп. работ не будет произведен.'
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

        if not engineer_projects.empty:
            months = calculate_by_month(engineer_projects, column="Баллы")
            results[engineer] = months
        else:
            results[engineer] = empty_months_df(column="Баллы")
            logging.info(
                f"Нет готовых доп. работ у проектировщика {engineer}. "
                f"Переходим к следующему проектировщику через 10 секунд."
            )

        send_add_work_data_to_spreadsheet(engineer_projects, engineer, archive_points)
        time.sleep(AFTER_ENG_SLEEP)

    return results
