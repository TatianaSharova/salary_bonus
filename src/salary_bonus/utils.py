from datetime import datetime as dt
from datetime import timedelta

import gspread
import holidays
import pandas as pd
from pandas.core.frame import DataFrame

from src.salary_bonus.logger import logging
from src.salary_bonus.worksheets.worksheets import (
    connect_to_project_archive,
    connect_to_settings_ws,
)

pd.options.mode.chained_assignment = None


async def get_project_archive_data() -> DataFrame | None:
    """
    Получает данные из архива проектов.
    Возвращает датафрейм с данными или None, если данных нет.
    """
    try:
        worksheet = connect_to_project_archive()
    except gspread.exceptions.SpreadsheetNotFound as err:
        logging.exception(err)
        return

    df = pd.DataFrame(worksheet.get_all_records(numericise_ignore=["all"]))

    return df


def get_employees() -> dict[str, list[str] | dict[str, list[str]]]:
    """
    Получает информацию о сотрудниках, для которых надо делать рассчет
    с листа 'Настройки'.
    Возвращает данные в формате:
        dict: {"engineers": ["name", "name2"],
               "lead": {
                   "lead_name": ["name"],
                   "lead_name2": ["name2"]
                   },
               "chief": ["name3"]
               }
        None: если нет данных
    """
    result = {"engineers": set(), "lead": {}, "chief": set()}

    sheet = connect_to_settings_ws()

    df = sheet.get("A1:C20")
    data_rows = pd.DataFrame(df[1:])

    if not data_rows.empty:
        for engineer, leader, chief in data_rows.values:
            # инженеры
            if engineer:
                engineer = engineer.strip()
                result["engineers"].add(engineer)

            # руководители
            if leader and engineer:
                leader = leader.strip()
                if leader not in result["lead"]:
                    result["lead"][leader] = set()
                result["lead"][leader].add(engineer)

            # ГИП
            if chief:
                result["chief"].add(chief.strip())

        result["engineers"] = sorted(result["engineers"])
        result["chief"] = sorted(result["chief"])

        for leader, engineers in result["lead"].items():
            result["lead"][leader] = sorted(engineers)

    return result


def is_point(s: str) -> bool:
    """
    Проверка столбца "Баллы".
    Если значение состоит из строки, в котрой только число, возвращает True.
    """
    try:
        float(s)
        return True
    except ValueError:
        return False


def count_non_working_days(start_date: dt.date, end_date: dt.date) -> int:
    """Считает количество нерабочих дней в заданном промежутке."""
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    ru_holidays = holidays.RU(years=range(start_date.year, end_date.year + 1))

    non_working_days = 0

    current_date = start_date
    while current_date <= end_date:
        if current_date.weekday() >= 5 or current_date in ru_holidays:
            non_working_days += 1
        current_date += timedelta(days=1)

    return non_working_days


def define_integer(integer: str) -> float | int:
    """
    Подготавливает введенные данные для дальнейших вычислений.
    Принимает строку, возвращает число.
    """
    integer = integer.replace(" ", "")
    integer = integer.replace("\xa0", "")
    try:
        integer = float(integer)
    except ValueError:
        if "," in integer:
            numbers = integer.split(",")
            integer = float(numbers[0])
        else:
            integer = 0
    return integer
