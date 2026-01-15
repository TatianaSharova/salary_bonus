import gspread
from gspread.spreadsheet import Spreadsheet
from gspread.worksheet import Worksheet
from pandas.core.frame import DataFrame

from src.salary_bonus.config.defaults import (
    ADD_WORK_COL_NAMES,
    AFTER_FORMAT_SLEEP,
    ARCHIVE_CURRENT_WS,
    BONUS_WS,
    CURRENT_YEAR,
    ENG_WS_COL_NAMES,
    RESULT_WS,
    SETTINGS_WS,
)
from src.salary_bonus.config.environment import CREDS_PATH, ENDPOINT_ATTENDANCE_SHEET
from src.salary_bonus.logger import logging
from src.salary_bonus.worksheets.google_sheets_manager import sheets_manager
from src.salary_bonus.worksheets.utils import (
    color_comp_correction,
    color_overdue_deadline,
    format_bonus_spreadsheet,
    format_new_engineer_ws,
    format_new_result_ws,
    format_settings_ws,
    get_column_letter,
)

gc = gspread.service_account(filename=CREDS_PATH)


def create_new_ws_archive(spreadsheet: Spreadsheet) -> Worksheet:
    """
    Создает новый лист для архива проектов/доп. работ
    с таким же форматированием, как у листа прошлого года.
    """
    logging.info(f"Создание нового листа {CURRENT_YEAR} в таблице проектов.")
    source_sheet = spreadsheet.worksheet(f"{int(CURRENT_YEAR) - 1}")
    source_sheet_title = source_sheet.title

    destination_spreadsheet_id = spreadsheet.id
    source_sheet.copy_to(destination_spreadsheet_id)

    new_sheet = spreadsheet.worksheet(f"{source_sheet_title} (копия)")

    new_sheet.update_title(CURRENT_YEAR)

    total_rows = new_sheet.row_count
    total_cols = new_sheet.col_count

    last_column_letter = get_column_letter(total_cols)

    new_sheet.batch_clear([f"A2:{last_column_letter}{total_rows}"])
    logging.info(f"Лист {CURRENT_YEAR} создан.")

    return new_sheet


def connect_to_archive(srpeadsheet_name: str) -> Worksheet:
    """
    Открывает лист с архивом проектов.
    При смене года создает новый лист.
    """
    archive_spreadsheet: Spreadsheet = sheets_manager.get_spreadsheet(srpeadsheet_name)

    ws = sheets_manager.get_worksheet(archive_spreadsheet, ARCHIVE_CURRENT_WS)

    if not ws:
        ws = create_new_ws_archive(archive_spreadsheet)

    return ws


def connect_to_settings_ws() -> Worksheet:
    """Открывает лист "Настройки" из таблицы "Премирование"."""
    spreadsheet: Spreadsheet = sheets_manager.get_or_create_spreadsheet(
        BONUS_WS, format_bonus_spreadsheet
    )
    ws = sheets_manager.get_or_create_worksheet(
        spreadsheet=spreadsheet, title=SETTINGS_WS, rows=100, formatter=format_settings_ws
    )
    return ws


def connect_to_engineer_ws(
    engineer: str, create_if_not_exist: bool = True
) -> Worksheet | None:
    """
    Открывает лист проектировщика.

    Args:
        engineer (str): Имя проектировщика (название листа)
        create_if_not_exist (bool): Создавать лист, если его нет

    Returns:
        Worksheet | None: Лист проектировщика или None, если лист не найден
    """
    spreadsheet: Spreadsheet = sheets_manager.get_or_create_spreadsheet(
        BONUS_WS, format_bonus_spreadsheet
    )

    if not create_if_not_exist:
        engineer_ws = sheets_manager.get_worksheet(spreadsheet, engineer)
    else:
        engineer_ws = sheets_manager.get_or_create_worksheet(
            spreadsheet,
            engineer,
            formatter=format_new_engineer_ws,
            sleep_after=AFTER_FORMAT_SLEEP,
        )

    return engineer_ws


def get_attendance_sheet_ws() -> list[Worksheet]:
    """Подключается к табелю посещаемости офиса и возвращает все листы."""
    spreadsheet: Spreadsheet = sheets_manager.get_spreadsheet_by_url(
        ENDPOINT_ATTENDANCE_SHEET
    )
    worksheets = sheets_manager.get_all_worksheets(spreadsheet)
    return worksheets


def send_project_data_to_spreadsheet(df: DataFrame, engineer: str) -> None:
    """
    Отправляет данные с баллами в таблицу "Премирование".
    """
    logging.info("Отправка данных о проектах на лист проектировщика.")
    sheet = connect_to_engineer_ws(engineer)

    eng_small = df[ENG_WS_COL_NAMES]

    # Очистка
    sheet.batch_clear(["A2:I200"])
    # Удаление форматирования
    sheet.format(
        "A2:I200",
        {
            "backgroundColor": {"red": 1, "green": 1, "blue": 1},
            "textFormat": {"bold": False},
        },
    )
    # Форматирование заголовка
    sheet.update([["Корректировка сложности"]], "J1")
    sheet.format(
        "A1:J1",
        {
            "backgroundColor": {"red": 0.7, "green": 1.0, "blue": 0.7},
            "textFormat": {"bold": True},
        },
    )

    sheet.update([eng_small.columns.values.tolist()] + eng_small.values.tolist())

    color_overdue_deadline(eng_small, sheet)
    color_comp_correction(df, sheet)


def send_add_work_data_to_spreadsheet(
    df: DataFrame, engineer: str, archive_data: dict[str, DataFrame]
) -> None:
    """
    Отправляет данные с баллами за доп. работы в таблицу "Премирование".
    """
    logging.info("Отправка данных о доп. работах на лист проектировщика.")
    sheet = connect_to_engineer_ws(engineer)

    eng_small = df[ADD_WORK_COL_NAMES]

    if engineer in archive_data:
        main_projects_length = len(archive_data[engineer])
        start_row = main_projects_length + 4
        sheet.update(
            [eng_small.columns.values.tolist()] + eng_small.values.tolist(),
            range_name=f"A{start_row}:H200",
        )
        sheet.format(
            f"A{start_row}:H{start_row}",
            {
                "backgroundColor": {"red": 1.0, "green": 0.85, "blue": 0.6},
                "textFormat": {"bold": True},
            },
        )
        start_row += 1
    else:
        sheet.format(
            "A1:H1",
            {
                "backgroundColor": {"red": 1.0, "green": 0.85, "blue": 0.6},
                "textFormat": {"bold": True},
            },
        )
        sheet.format(
            "I1:J1",
            {
                "backgroundColor": {"red": 1.0, "green": 1, "blue": 1},
                "textFormat": {"bold": False},
            },
        )
        sheet.batch_clear(["I1:J1"])
        sheet.update([eng_small.columns.values.tolist()] + eng_small.values.tolist())
        start_row = 2

    color_overdue_deadline(eng_small, sheet, start_row)


def send_month_data_to_spreadsheet(df: DataFrame, engineer: str) -> None:
    """
    Отсылает данные о баллах, заработанных в каждом месяце
    в таблицу "Премирование".
    """
    logging.info(
        f"Отправка данных о баллах по месяцам на лист проектировщика {engineer}."
    )
    sheet = connect_to_engineer_ws(engineer)

    sheet.update([df.columns.values.tolist()] + df.values.tolist(), range_name="L1:M13")


def send_results_data_ws(df: DataFrame) -> None:
    """Отправляет данные о средних баллах на лист "Итоги"."""
    spreadsheet: Spreadsheet = sheets_manager.get_or_create_spreadsheet(
        BONUS_WS, format_bonus_spreadsheet
    )

    result_ws = sheets_manager.get_or_create_worksheet(
        spreadsheet,
        RESULT_WS,
        cols=40,
        formatter=format_new_result_ws,
        sleep_after=AFTER_FORMAT_SLEEP,
    )

    logging.info('Отправка данных о средних баллах на лист "Итоги".')
    result_ws.update(
        [df.columns.values.tolist()] + df.values.tolist(), range_name="P1:Q15"
    )


def send_hours_data_ws(df: DataFrame) -> None:
    """Отправляет данные о рабочих часах на лист итогов."""
    spreadsheet: Spreadsheet = sheets_manager.get_or_create_spreadsheet(
        BONUS_WS, format_bonus_spreadsheet
    )

    result_ws = sheets_manager.get_or_create_worksheet(
        spreadsheet,
        RESULT_WS,
        cols=40,
        formatter=format_new_result_ws,
        sleep_after=AFTER_FORMAT_SLEEP,
    )

    logging.info('Отправка данных о рабочих часах на лист "Итоги".')
    result_ws.update(
        [df.columns.values.tolist()] + df.values.tolist(), range_name="S1:AE30"
    )


def send_lead_res_to_ws(
    leads_data: dict[str, DataFrame], gip_df: DataFrame | None
) -> None:
    """Отправляет итоги руководителей группы на лист итогов."""
    spreadsheet: Spreadsheet = sheets_manager.get_or_create_spreadsheet(
        BONUS_WS, format_bonus_spreadsheet
    )

    ws: Worksheet = sheets_manager.get_or_create_worksheet(
        spreadsheet, RESULT_WS, cols=40, formatter=format_new_result_ws
    )

    logging.info('Отправка данных по руководителям на лист "Итоги".')

    rows = []
    merge_rows: list[int] = []

    current_row = 1  # считаем строки, начиная с 1

    for lead_name, df in leads_data.items():
        # строка с именем руководителя
        rows.append([lead_name] + [""] * 12)
        merge_rows.append(current_row)
        current_row += 1

        # заголовки DataFrame
        rows.append(["Имя"] + df.columns.tolist())
        current_row += 1

        # данные
        for idx, row in df.iterrows():
            rows.append([idx] + row.tolist())
            current_row += 1

        # пустая строка
        rows.append([""] * 13)
        current_row += 1

    # --- добавляем ГИП внизу, если передали df ---
    if gip_df is not None and not gip_df.empty:
        rows.append(["ГИП"] + [""] * 12)
        merge_rows.append(current_row)
        current_row += 1

        rows.append([""] + gip_df.columns.tolist())
        current_row += 1

        for idx, row in gip_df.iterrows():
            rows.append([idx] + row.tolist())
            current_row += 1

        rows.append([""] * 13)
        current_row += 1
    # --- /ГИП ---

    # очистка диапазона
    ws.batch_clear(["A1:N200"])
    ws.unmerge_cells("A1:N200")
    ws.format(
        "A1:N200",
        {
            "backgroundColor": {"red": 1, "green": 1, "blue": 1},
            "textFormat": {"bold": False},
        },
    )

    # отправка данных
    ws.update(rows, range_name="A1:N200")

    # объединение ячеек под имена руководителей/ГИП
    for row_num in merge_rows:
        ws.merge_cells(f"A{row_num}:N{row_num}")
        ws.format(
            f"A{row_num}:H{row_num}",
            {
                "backgroundColor": {"red": 1, "green": 0.8, "blue": 0.8},
                "textFormat": {"bold": True},
            },
        )
