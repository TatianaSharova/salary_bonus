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
            return "Неизвестный тип работ, невозможно сделать рассчёт"

    if "Расстановка оборудования" in work_types:
        points += 0.2
    if "Подготовка спецификации" in work_types:
        points += 0.2
    if "ГР Модульная установка":
        points += check_gr(
            row,
        )

    # points += check_amount_directions(complexity, row["Количество направлений"])
    # points += check_square(complexity, row)
    # points += check_sot_skud(row)
    # points += check_cultural_heritage(row)
    # points += check_net(row)
    # points = points / check_authors(row["Разработал"])

    points = check_spend_time(row, points, df, amount=0, archive_sh=False)

    if isinstance(points, str):
        return points

    return round(points, 1)


def check_gr(row: pd.Series) -> int:
    """
    Docstring for check_gr

    :param row: Description
    :type row: pd.Series
    :return: Description
    :rtype: int
    """
    # points = 0
