from datetime import datetime as dt
from datetime import timedelta

import gspread
import holidays
import pandas as pd
from pandas.core.frame import DataFrame

from src.salary_bonus.logger import logging
from src.salary_bonus.notification.telegram.bot import send_tg_message, tg_bot
from src.salary_bonus.worksheets.worksheets import (
    connect_to_project_archive,
    connect_to_settings_ws,
)

pd.options.mode.chained_assignment = None


async def get_project_archive_data() -> DataFrame:
    """
    Получает данные из архива проектов.
    Возвращает датафрейм с данными или None, если данных нет.
    """
    try:
        worksheet = connect_to_project_archive()
    except gspread.exceptions.SpreadsheetNotFound as err:
        logging.exception(err)
        await send_tg_message(
            tg_bot,
            'Ошибка: таблица "Таблица проектов" не найдена.\n'
            "Возможно название было сменено.",
        )
        return

    df = pd.DataFrame(worksheet.get_all_records(numericise_ignore=["all"]))

    if df.empty:
        msg = "Таблица проектов пуста, расчет не будет произведен."
        logging.warning(msg)
        await send_tg_message(tg_bot, msg)

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


# def non_count_engineers() -> list[str]: TODO: del
#     """
#     Возвращает список проектировщиков, для которых не надо делать
#     премиальный расчет.
#     """
#     sheet = connect_to_settings_ws()

#     df = sheet.get("A1:A20")
#     non_count_eng = pd.DataFrame(df[1:], columns=df[0])
#     if not non_count_eng.empty:
#         non_count_eng_list = non_count_eng["Не учитывать"].tolist()
#         logging.info(
#             f"Cписок проектировщиков, для которых не "
#             f"надо делать расчет: {non_count_eng_list}"
#         )
#         return non_count_eng_list
#     return None


# def get_list_of_engineers(df: DataFrame, colomn: str) -> list[str]:
#     """
#     Принимает датафрейм c таблицы проектов и название столбца,
#     из которого брать фамилии проектировщиков.
#     Возвращает список проектировщиков.
#     """
#     engineers = set(df[colomn])
#     non_count_eng = non_count_engineers()
#     groups_of_engineers = set()
#     groups = set()

#     if "" in engineers:
#         engineers.remove("")

#     for engineer in engineers:
#         if "," in engineer:
#             groups_of_engineers.add(engineer)

#     for group in groups_of_engineers:
#         groups.update(group.split(", "))
#         engineers.remove(group)

#     union_eng = engineers.union(groups)

#     if not non_count_eng:
#         return list(union_eng)
#     unique_eng = union_eng - set(non_count_eng)
#     return list(unique_eng)


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
