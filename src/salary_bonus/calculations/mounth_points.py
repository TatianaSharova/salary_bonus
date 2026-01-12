from typing import Any, Dict

import pandas as pd
from pandas import DataFrame, Series

from src.salary_bonus.config.defaults import CURRENT_YEAR
from src.salary_bonus.logger import logging


def calculate_month_points(row: Series, column: str) -> Dict[str, Any] | None:
    """
    Формирует запись для последующей агрегации значений по месяцам.

    Args:
      row: Одна строка исходного DataFrame (pandas.Series)
            Ожидаемая структура строки DataFrame (`row`):
                - "Дата окончания проекта": str | datetime
                    Дата завершения проекта в формате "ДД.ММ.ГГГГ"
                - column (str): int | float
                    Колонка с числовым значением,
                    название которой передаётся аргументом `column`
      column: Название столбца DataFrame, содержащего числовое значение

    Returns:
      Словарь вида:
        {
            "Месяц": pandas.Period("YYYY-MM", freq="M"),
            <column>: int | float
        } или None, если дата некорректна
    """
    end_date = pd.to_datetime(
        row["Дата окончания проекта"], dayfirst=True, format="%d.%m.%Y", errors="coerce"
    )

    if pd.isna(end_date):
        return None

    month_period = end_date.to_period("M")

    return {
        "Месяц": month_period,
        column: row[column],
    }


def empty_months_df(column: str) -> pd.DataFrame:
    months = pd.period_range(
        start=f"{CURRENT_YEAR}-01", end=f"{CURRENT_YEAR}-12", freq="M"
    )
    return pd.DataFrame(
        {"Месяц": [f"{m.month:02d}-{m.year}" for m in months], column: [0.0] * 12}
    )


def calculate_by_month(df: DataFrame, column: str) -> DataFrame:
    """
    Агрегирует числовые значения указанного столбца по месяцам окончания проектов.

    Ожидаемая структура входного DataFrame (`df`):
        - "Дата окончания проекта": str | datetime
        - column (str): int | float
            Столбец, имя которого передаётся аргументом `column`
            и содержит значения для агрегации

    Логика:
        - Каждый проект учитывается ровно один раз
        - Значение из столбца `column` полностью относится
          к месяцу завершения проекта
        - Значения суммируются по месяцам
        - В результат включаются только месяцы текущего года

    :param df: Исходный DataFrame с данными по проектам
    :param column: Название столбца DataFrame, который необходимо агрегировать
    :return: DataFrame с агрегированными значениями по месяцам
    Структура итогового DataFrame:
        - "Месяц": str
            Месяц в формате "MM-YYYY"
        - column (str): float
            Суммарное значение за месяц
    """
    logging.info(
        "Агрегация значений столбца '%s' по месяцам окончания проектов.",
        column,
    )

    monthly_points = df.apply(
        calculate_month_points,
        axis=1,
        args=(column,),
    )
    monthly_points = [item for item in monthly_points if item is not None]

    if not monthly_points:
        # Формируем пустую таблицу с нулями на все месяцы текущего года
        return empty_months_df(column)

    monthly_df = pd.DataFrame(monthly_points)

    result = monthly_df.groupby("Месяц", as_index=False)[column].sum()

    result["Месяц"] = result["Месяц"].apply(
        lambda period: f"{period.month:02d}-{period.year}"
    )

    # Создаем полный ряд месяцев текущего года
    months = pd.period_range(
        start=f"{CURRENT_YEAR}-01", end=f"{CURRENT_YEAR}-12", freq="M"
    )
    full_df = pd.DataFrame({"Месяц": [f"{m.month:02d}-{m.year}" for m in months]})

    # Объединяем и заполняем пропуски нулями
    final_df = full_df.merge(result, on="Месяц", how="left").fillna(0)
    final_df[column] = final_df[column].astype(float)

    return final_df
