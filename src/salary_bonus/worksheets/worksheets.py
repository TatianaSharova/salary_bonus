from datetime import datetime as dt

import gspread
from gspread.spreadsheet import Spreadsheet
from gspread.worksheet import Worksheet
from pandas.core.frame import DataFrame

from src.salary_bonus.config.defaults import (
    AFTER_FORMAT_SLEEP,
    ARCHIVE_CURRENT_WS,
    BONUS_WS,
    ENG_WS_COL_NAMES,
    FIRST_SHEET,
    PROJECT_ARCHIVE,
    SETTINGS_WS,
)
from src.salary_bonus.config.environment import (
    CREDS_PATH,
    EMAILS,
    ENDPOINT_ATTENDANCE_SHEET,
)
from src.salary_bonus.exceptions import NonValidEmailsError
from src.salary_bonus.logger import logging
from src.salary_bonus.worksheets.google_sheets_manager import sheets_manager
from src.salary_bonus.worksheets.utils import (
    color_comp_correction,
    color_overdue_deadline,
    format_new_engineer_ws,
    format_settings_ws,
    get_column_letter,
)

gc = gspread.service_account(filename=CREDS_PATH)


def create_new_ws_project_archive(spreadsheet: Spreadsheet) -> Worksheet:
    """
    Создает новый лист для архива проектов
    с таким же форматированием, как у листа прошлого года.
    """
    logging.info(f"Создание нового листа {dt.now().year} в таблице проектов.")
    source_sheet = spreadsheet.worksheet(f"{dt.now().year - 1}")
    source_sheet_title = source_sheet.title

    destination_spreadsheet_id = spreadsheet.id
    source_sheet.copy_to(destination_spreadsheet_id)

    new_sheet = spreadsheet.worksheet(f"{source_sheet_title} (копия)")

    new_sheet.update_title(f"{dt.now().year}")

    total_rows = new_sheet.row_count
    total_cols = new_sheet.col_count

    last_column_letter = get_column_letter(total_cols)

    new_sheet.batch_clear([f"A2:{last_column_letter}{total_rows}"])
    logging.info(f"Лист {dt.now().year} создан.")

    return new_sheet


def connect_to_project_archive() -> Worksheet:
    """
    Открывает лист с архивом проектов.
    При смене года создает новый лист.
    """
    archive_spreadsheet: Spreadsheet = sheets_manager.get_spreadsheet(PROJECT_ARCHIVE)

    ws = sheets_manager.get_worksheet(archive_spreadsheet, ARCHIVE_CURRENT_WS)

    if not ws:
        ws = create_new_ws_project_archive(archive_spreadsheet)

    return ws


def format_bonus_spreadsheet(spreadsheet: Spreadsheet) -> None:
    for email in EMAILS.split():
        try:
            spreadsheet.share(email, perm_type="user", role="writer", notify=True)
        except gspread.exceptions.APIError as error:
            logging.exception("Переданы невалидные emails в .env")
            raise NonValidEmailsError(error)

    sheets_manager.get_or_create_worksheet(
        spreadsheet=spreadsheet, title=SETTINGS_WS, rows=100, formatter=format_settings_ws
    )
    sheet1 = spreadsheet.worksheet(FIRST_SHEET)
    spreadsheet.del_worksheet(sheet1)

    sheets_manager.invalidate_spreadsheet(spreadsheet)


def connect_to_settings_ws() -> Worksheet:
    """Открывает лист "Настройки" из таблицы "Премирование"."""
    spreadsheet: Spreadsheet = sheets_manager.get_or_create_spreadsheet(
        BONUS_WS, format_bonus_spreadsheet
    )
    ws = sheets_manager.get_or_create_worksheet(
        spreadsheet=spreadsheet, title=SETTINGS_WS, rows=100, formatter=format_settings_ws
    )
    return ws


def connect_to_engineer_ws(engineer: str) -> Worksheet | None:
    """
    Открывает лист проектировщика.
    Если лист не найден, возвращает None.
    """
    spreadsheet: Spreadsheet = sheets_manager.get_or_create_spreadsheet(
        BONUS_WS, format_bonus_spreadsheet
    )
    engineer_ws = sheets_manager.get_worksheet(spreadsheet, engineer)

    return engineer_ws


def connect_to_engineer_ws_or_create(engineer: str) -> Worksheet:
    """
    Открывает лист проектировщика.
    Если лист не найден, создает его.
    """
    spreadsheet: Spreadsheet = sheets_manager.get_or_create_spreadsheet(
        BONUS_WS, format_bonus_spreadsheet
    )
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
    sheet = connect_to_engineer_ws_or_create(engineer)

    eng_small = df[ENG_WS_COL_NAMES]

    sheet.update([eng_small.columns.values.tolist()] + eng_small.values.tolist())

    color_overdue_deadline(eng_small, sheet)
    color_comp_correction(df, sheet)


def send_quarter_data_to_spreadsheet(df: DataFrame, engineer: str) -> None:
    """
    Отсылает данные о баллах, заработанных в каждом квартале
    в таблицу "Премирование".
    """
    logging.info("Отправка данных о баллах по кварталам на лист проектировщика.")
    sheet = connect_to_engineer_ws_or_create(engineer)

    sheet.update([df.columns.values.tolist()] + df.values.tolist(), range_name="L1:N200")


def send_results_data_ws(df: DataFrame) -> None:
    """Отправляет данные о средних баллах на лист "Настройки"."""
    worksheet = connect_to_settings_ws()

    logging.info('Отправка данных о средних баллах на лист "Настройки".')
    worksheet.update(
        [df.columns.values.tolist()] + df.values.tolist(), range_name="C1:E20"
    )


def send_bonus_data_ws(engineer: str, df: DataFrame) -> None:
    """Отправляет данные о выполнении плана на лист проектировщика."""
    worksheet = connect_to_engineer_ws_or_create(engineer)

    logging.info(f"Отправка данных о выполнении плана проектировщика {engineer}.")
    worksheet.update(
        [df.columns.values.tolist()] + df.values.tolist(), range_name="N1:Q20"
    )


def send_hours_data_ws(df: DataFrame) -> None:
    """Отправляет данные о рабочих часах на лист настроек."""
    worksheet = connect_to_settings_ws()

    logging.info('Отправка данных о рабочих часах на лист "Настройки".')
    worksheet.update(
        [df.columns.values.tolist()] + df.values.tolist(), range_name="G1:S30"
    )
