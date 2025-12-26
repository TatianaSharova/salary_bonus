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
from src.salary_bonus.logger import logging
from src.salary_bonus.notification.telegram.bot import TelegramNotifier
from src.salary_bonus.utils import get_employees, get_project_archive_data
from src.salary_bonus.worksheets.google_sheets_manager import sheets_manager

pd.options.mode.chained_assignment = None
pd.set_option("future.no_silent_downcasting", True)


async def main() -> None:
    """
    Запускает и завершает работу программы.
    """
    logging.info("Запущена основная задача.")
    tg_bot = TelegramNotifier()

    df = get_project_archive_data()

    if df is None:
        await tg_bot.send_message(
            'Ошибка: таблица "Таблица проектов" не найдена.\n'
            "Возможно название было сменено.",
        )
        sheets_manager.invalidate()
        await tg_bot.close()
        return
    elif isinstance(df, pd.DataFrame) and df.empty:
        msg = "Таблица проектов пуста, расчет не будет произведен."
        logging.warning(msg)
        await tg_bot.send_message(msg)
        sheets_manager.invalidate()
        await tg_bot.close()
        return

    try:
        employees_data = get_employees()
        list_of_engineers = employees_data["engineers"]

        # Рассчет баллов по основным проектам для проектировщиков
        if len(list_of_engineers) > 0:
            archive_points = process_project_archive_data(list_of_engineers, df)
        else:
            msg = (
                "Нет данных о проектировщиках для расчета на листе 'Настройки'."
                " Расчет не будет произведен."
            )
            logging.warning(
                msg + f"\n\nПолученные данные с листа 'Настройки':\n {employees_data}"
            )
            await tg_bot.send_message(msg)
            sheets_manager.invalidate()
            await tg_bot.close()
            return

        # Рассчет баллов по дополнительным проектам для проектировщиков
        process_additional_work_data(archive_points, list_of_engineers, tg_bot)
        # TODO

        # Рассчет баллов для руководителей
        if len(list(employees_data["lead"].keys())) > 0:
            process_lead_data(archive_points, employees_data["lead"])
        else:
            msg = (
                "Баллы проектировщиков посчитаны и отправлены на листы проектировщиков."
                " Для расчета баллов руководителей не найдено данных."
            )
            logging.warning(msg)
            await tg_bot.send_message(msg)
            sheets_manager.invalidate()
            await tg_bot.close()

        gip = employees_data["chief"]
        if len(gip) > 0:
            ...
        else:
            msg = (
                "Баллы для руководителей посчитаны и отправлены на листы руководителей. "
                " Для расчета баллов главных инженеров не найдено данных."
            )
            logging.warning(msg)
            await tg_bot.send_message(msg)
            sheets_manager.invalidate()
            await tg_bot.close()

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
