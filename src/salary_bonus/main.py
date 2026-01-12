import asyncio
import os
import subprocess
import sys
import traceback
from datetime import datetime, timedelta

import pandas as pd
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone

sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from src.salary_bonus.calculations.additional_archive.process import (
    process_additional_work_data,
)
from src.salary_bonus.calculations.lead_results import process_lead_data
from src.salary_bonus.calculations.project_archive.process import (
    process_project_archive_data,
)
from src.salary_bonus.calculations.results import do_results
from src.salary_bonus.calculations.utils import find_sum_equipment
from src.salary_bonus.logger import logging
from src.salary_bonus.notification.telegram.bot import TelegramNotifier
from src.salary_bonus.utils import (
    get_employees,
    get_project_archive_data,
    sum_points_by_month,
)
from src.salary_bonus.worksheets.google_sheets_manager import sheets_manager
from src.salary_bonus.worksheets.worksheets import send_month_data_to_spreadsheet

pd.options.mode.chained_assignment = None
pd.set_option("future.no_silent_downcasting", True)


async def main() -> None:
    """
    Запускает и завершает работу программы.
    """
    logging.info("Запущена основная задача.")
    tg_bot = TelegramNotifier()

    try:
        employees_data = get_employees()
        list_of_engineers = employees_data["engineers"]

        if len(list_of_engineers) == 0:
            msg = (
                "Нет данных о проектировщиках для расчета на листе 'Настройки'."
                " Расчет не будет произведен."
            )
            logging.warning(
                msg + f'\n\nПолученные данные с листа "Настройки":\n {employees_data}'
            )
            await tg_bot.send_message(msg)
            return

        # Расчет баллов по основным проектам для проектировщиков
        main_archive_df = get_project_archive_data()
        archive_points, eng_data = await process_project_archive_data(
            main_archive_df, list_of_engineers, tg_bot
        )

        # Рассчет баллов по дополнительным проектам для проектировщиков
        add_data_points = await process_additional_work_data(
            list_of_engineers, tg_bot, eng_data
        )

        # Суммируем результаты из двух источников и отправляем в таблицы
        month_res_data = sum_points_by_month(archive_points, add_data_points)
        for engineer, df in month_res_data.items():
            send_month_data_to_spreadsheet(df, engineer)

        # Считаем средние баллы и часы работы и отправляем на лист "Итоги"
        sum_equipment = find_sum_equipment(main_archive_df)
        do_results(month_res_data, sum_equipment)

        # Рассчет баллов для руководителей и гипа
        process_lead_data(archive_points, employees_data["lead"], employees_data["chief"])

        await tg_bot.send_message("Расчет баллов окончен.")
    except Exception as error:
        logging.exception(error)
        error_name = type(error).__name__
        tb = "".join(traceback.format_tb(error.__traceback__))

        await tg_bot.send_message(
            f"Во время расчета произошла ошибка {error_name}: {error}\n\n" f"{tb}"
        )
    finally:
        sheets_manager.invalidate()
        await tg_bot.close()


async def update_holidays_package():
    """
    Обновляет пакет holidays для подгрузки данных о выходных в новых годах.
    """
    tg_bot = TelegramNotifier()
    try:
        subprocess.run(["pip", "install", "--upgrade", "holidays"], check=True)
        logging.info("Библиотека holidays была обновлена.")
    except subprocess.CalledProcessError as error:
        logging.exception(f"Библиотека holidays не обновлена: {error}")
        await tg_bot.send_message(f"Ошибка при обновлении holidays library: {error}")
    finally:
        await tg_bot.close()


def setup_scheduler():
    """
    Запускает планировщик. Задача выполнится сразу после запуска,
    а потом будет каждый день в 10:00 утра.
    """
    scheduler = AsyncIOScheduler(timezone="Asia/Dubai")

    samara_tz = timezone("Asia/Dubai")

    scheduler.add_job(
        main,
        trigger="date",
        next_run_time=datetime.now(samara_tz) + timedelta(seconds=2),
        misfire_grace_time=120,
    )

    scheduler.add_job(
        main, CronTrigger(hour=10, minute=0, timezone=samara_tz), misfire_grace_time=60
    )

    scheduler.add_job(
        update_holidays_package,
        "cron",
        month="6,12",
        day=1,
        hour=9,
        minute=0,
        misfire_grace_time=60,
        timezone=samara_tz,
    )

    scheduler.start()


if __name__ == "__main__":
    setup_scheduler()

    try:
        asyncio.get_event_loop().run_forever()
    except KeyboardInterrupt:
        pass
    except SystemExit as err:
        logging.critical(err)
