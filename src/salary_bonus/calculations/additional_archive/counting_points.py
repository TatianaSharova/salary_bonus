import pandas as pd

from src.salary_bonus.calculations.additional_archive.utils import (
    check_add_filled_projects,
)
from src.salary_bonus.calculations.project_archive.counting_points import check_spend_time
from src.salary_bonus.config.defaults import ADD_WORK_TYPES


def count_add_points(row: pd.Series, df: pd.DataFrame) -> int | str:
    """
    Считает и возвращает сумму полученных баллов.
    Если подсчет произвести невозможно, то возвращает
    строку-предупреждение об этом.
    """
    points = 0
    filled_project = check_add_filled_projects(row)
    if not filled_project:
        return "Необходимо заполнить данные для расчёта"

    work_types = row["Тип работы"].split(", ")

    for w_type in work_types:
        if w_type not in ADD_WORK_TYPES:
            return "Неизвестный тип работы, невозможно сделать рассчёт"

    if "Расстановка оборудования" in work_types:
        points += 0.2
    if "Подготовка спецификации" in work_types:
        points += 0.2
    if "ГР Модульная установка" in work_types:
        points += check_gr(row)
    if "ГР Модульная установка 500+" in work_types:
        points += check_gr(row)
        points += 1.5
    if "ГР Централизованная установка" in work_types:
        points += check_gr_center(row)  # TODO: check how to count

    points = check_spend_time(row, points, df, amount=0, archive_sh=False)

    if isinstance(points, str):
        return points

    return round(points, 1)


def check_gr(row: pd.Series) -> int | float:
    """
    Начисление баллов за количество направлений при ГР.

    Args:
        row (pd.Series): строка с данными проекта
    """
    try:
        amount_directions = int(row["Количество направлений"])
    except ValueError:
        amount_directions = 0

    if amount_directions > 20:
        return 1
    elif 20 >= amount_directions > 10:
        return 0.5
    elif amount_directions < 11:
        return 0.2


def check_gr_center(row: pd.Series) -> float | int:
    """
    Начисление баллов за количество направлений при ГР
    централизованной установки.

    Args:
        row (pd.Series): строка с данными проекта
    """
    try:
        amount_directions = int(row["Количество направлений"])
    except ValueError:
        amount_directions = 0

    if amount_directions <= 15:
        return 1.5
    else:
        return 2
