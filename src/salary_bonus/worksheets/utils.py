from datetime import datetime as dt

from gspread.worksheet import Worksheet
from gspread_formatting import set_column_widths, set_frozen
from pandas import DataFrame

from src.salary_bonus.logger import logging


def get_column_letter(n: int) -> str:
    """
    Преобразует номер столбца в буквенное обозначение.
    Например, 1 -> A, 27 -> AA.
    """
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string


def color_overdue_deadline(df: DataFrame, sheet: Worksheet) -> None:
    """Окрашивает ячейки с просроченным дедлайном."""
    logging.info('Окраска ячеек в столбце "Дедлайн" с просроченным дедлайном.')
    sheet.format(
        "H2:H200",
        {
            "backgroundColor": {"red": 1, "green": 1, "blue": 1},
        },
    )
    for index, row in df.iterrows():
        try:
            end_date = dt.strptime(row["Дата окончания проекта"], "%d.%m.%Y").date()
            deadline = dt.strptime(row["Дедлайн"], "%d.%m.%Y").date()

            if deadline < end_date:
                sheet.format(
                    f"H{index + 2}",
                    {
                        "backgroundColor": {"red": 1, "green": 0.8, "blue": 0.8},
                    },
                )
        except ValueError:
            try:
                deadline = dt.strptime(row["Дедлайн"], "%d.%m.%Y").date()

                if deadline < dt.now().date():
                    sheet.format(
                        f"H{index + 2}",
                        {
                            "backgroundColor": {"red": 1, "green": 0.8, "blue": 0.8},
                        },
                    )
            except ValueError:
                continue


def color_comp_correction(df: DataFrame, sheet: Worksheet) -> None:
    """Окрашивает ячейки с учтенной коррекцией сложности."""
    logging.info(
        "Окраска ячеек с учтенной коррекцией сложности "
        'в столбце "Корректировка сложности".'
    )
    sheet.format(
        "J2:J200",
        {
            "backgroundColor": {"red": 1, "green": 1, "blue": 1},
        },
    )
    if "Корректировка сложности" in df.columns:
        for index, row in df.iterrows():
            if (
                row["Корректировка сложности"]
                and isinstance(row["Корректировка сложности"], str)
                and row["Корректировка сложности"].isdigit()
            ):
                sheet.format(
                    f"J{index + 2}",
                    {
                        "backgroundColor": {"red": 1, "green": 1, "blue": 0.8},
                    },
                )


def format_new_engineer_ws(sheet: Worksheet) -> None:
    """Форматирует новый лист для инженера."""
    set_column_widths(
        sheet, [("A", 100), ("B", 400), ("C", 200), ("D:G", 150), ("I:J", 150)]
    )
    sheet.update([["Корректировка сложности"]], "J1")
    sheet.update([["Отпуск/отгул/не работал(а) (в часах)"]], "Q1")
    sheet.format(
        "A1:J1",
        {
            "backgroundColor": {"red": 0.7, "green": 1.0, "blue": 0.7},
            "textFormat": {"bold": True},
        },
    )
    sheet.format(
        "A1:T200",
        {
            "wrapStrategy": "WRAP",
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE",
        },
    )
    sheet.format(
        "L1:Q1",
        {
            "backgroundColor": {"red": 0.8, "green": 0.9, "blue": 1},
            "textFormat": {"bold": True},
        },
    )
    set_frozen(sheet, rows=1)


def format_settings_ws(sheet: Worksheet) -> None:
    """Форматирует новый лист настроек."""
    sheet.update([["Не учитывать"]], "A1")
    sheet.format(
        "A1:T40",
        {
            "wrapStrategy": "WRAP",
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE",
        },
    )
    sheet.format(
        ["A1", "C1:E1", "G1:S1"],
        {
            "backgroundColor": {"red": 1, "green": 0.8, "blue": 0.8},
            "textFormat": {"fontSize": 12},
        },
    )
    set_column_widths(sheet, [("A", 300), ("E", 120)])
