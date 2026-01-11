from datetime import datetime as dt
from datetime import timedelta

import gspread
import holidays
import pandas as pd
from pandas.core.frame import DataFrame

from src.salary_bonus.config.defaults import ADDITIONAL_WORK, PROJECT_ARCHIVE
from src.salary_bonus.logger import logging
from src.salary_bonus.worksheets.worksheets import (
    connect_to_archive,
    connect_to_settings_ws,
)

pd.options.mode.chained_assignment = None


def get_project_archive_data() -> DataFrame | None:
    """
    Получает данные из архива проектов.
    Возвращает датафрейм с данными или None, если таблица не найдена.
    """
    try:
        worksheet = connect_to_archive(PROJECT_ARCHIVE)
    except gspread.exceptions.SpreadsheetNotFound as err:
        logging.exception(err)
        return None

    df = pd.DataFrame(worksheet.get_all_records(numericise_ignore=["all"]))

    return df


def get_add_work_data() -> DataFrame | None:
    """
    Получает данные из архива доп. работ.
    Возвращает датафрейм с данными или None, если данных нет.
    """
    try:
        worksheet = connect_to_archive(ADDITIONAL_WORK)
    except gspread.exceptions.SpreadsheetNotFound as err:
        logging.exception(err)
        return None

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


def sum_points_by_month(
    d1: dict[str, DataFrame],
    d2: dict[str, DataFrame],
    month_col: str = "Месяц",
    points_col: str = "Баллы",
) -> dict[str, DataFrame]:
    """
    Суммирует баллы по месяцам для каждого проектировщика из двух словарей.

    Ожидается, что в df есть колонки:
      - month_col: pandas.Period('YYYY-MM', freq='M') или строка вида 'YYYY-MM'
      - points_col: float

    Возвращает словарь engineer -> df с колонками [month_col, points_col],
    где points_col — сумма из обоих источников по каждому месяцу.
    """
    result: dict[str, DataFrame] = {}

    all_engineers = set(d1) | set(d2)

    for eng in all_engineers:
        frames: list[DataFrame] = []
        if eng in d1 and d1[eng] is not None and not d1[eng].empty:
            frames.append(d1[eng][[month_col, points_col]].copy())
        if eng in d2 and d2[eng] is not None and not d2[eng].empty:
            frames.append(d2[eng][[month_col, points_col]].copy())

        if not frames:
            result[eng] = pd.DataFrame(columns=[month_col, points_col])
            continue

        df = pd.concat(frames, ignore_index=True)

        df[points_col] = pd.to_numeric(df[points_col], errors="coerce").fillna(0.0)

        summed = (
            df.groupby(month_col, as_index=False)[points_col]
            .sum()
            .sort_values(month_col)
            .reset_index(drop=True)
        )

        result[eng] = summed

    return result
