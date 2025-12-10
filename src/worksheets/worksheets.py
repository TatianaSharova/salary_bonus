import time
from datetime import datetime as dt

import gspread
from gspread.spreadsheet import Spreadsheet
from gspread.worksheet import Worksheet
from pandas.core.frame import DataFrame

from config.environment import CREDS_PATH, EMAILS, ENDPOINT_ATTENDANCE_SHEET
from exceptions import NonValidEmailsError
from logger import logging
from worksheets.utils import (
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
    source_sheet.copy_to(f"{destination_spreadsheet_id}")

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
    logging.info(f"Подключение к таблице проектов, к листу {dt.now().year}.")
    try:
        worksheet = gc.open("Таблица проектов").worksheet(f"{dt.now().year}")
    except gspread.exceptions.WorksheetNotFound:
        logging.info(f"Лист {dt.now().year} не найден.")
        spreadsheet = gc.open("Таблица проектов")
        worksheet = create_new_ws_project_archive(spreadsheet)

    return worksheet


def add_settings_ws(spreadsheet: Spreadsheet) -> Worksheet:
    """Создает лист "Настройки" и форматирует его."""
    logging.info('Создание листа "Настройки".')

    sheet = spreadsheet.add_worksheet(title="Настройки", rows=100, cols=20)
    format_settings_ws(sheet)

    logging.info('Лист "Настройки" создан.')
    return sheet


def connect_to_bonus_ws() -> Spreadsheet:
    """
    Открывает таблицу "Премирование".
    При смене года создает новую таблицу.
    """
    logging.info(f'Подключение к таблице "Премирование{dt.now().year}".')
    try:
        spreadsheet = gc.open(f"Премирование{dt.now().year}")
    except gspread.exceptions.SpreadsheetNotFound:
        logging.info(
            f'Таблица "Премирование{dt.now().year}" не найдена. '
            f"Создание новой таблицы."
        )
        spreadsheet = gc.create(f"Премирование{dt.now().year}")
        for email in EMAILS.split():
            try:
                spreadsheet.share(email, perm_type="user", role="writer", notify=True)
            except gspread.exceptions.APIError as error:
                raise NonValidEmailsError(error)
        add_settings_ws(spreadsheet)
        sheet1 = spreadsheet.worksheet("Sheet1")
        spreadsheet.del_worksheet(sheet1)

    return spreadsheet


def connect_to_settings_ws() -> Worksheet:
    """Открывает лист "Настройки" из таблицы "Премирование"."""
    sheet = connect_to_bonus_ws()
    logging.info('Подключение к листу "Настройки".')
    try:
        settings = sheet.worksheet("Настройки")
    except gspread.exceptions.WorksheetNotFound:
        logging.info('Лист "Настройки" не найден.')
        settings = add_settings_ws(sheet)

    return settings


def create_engineer_ws(spreadsheet: Spreadsheet, engineer: str) -> Worksheet:
    """
    Создает для проектироващика лист и форматирует его.
    """
    logging.info(f'Создание листа {engineer} в таблице "Премирование".')

    sheet = spreadsheet.add_worksheet(title=f"{engineer}", rows=200, cols=20)

    format_new_engineer_ws(sheet)

    logging.info(f"Лист {engineer} создан. " f"Ждем 30 секунд для продолжения работы.")
    time.sleep(30)

    return sheet


def connect_to_engineer_ws(engineer: str) -> Worksheet:
    """
    Открывает лист проектировщика.
    Если лист не найден, возвращает None.
    """
    spreadsheet = connect_to_bonus_ws()

    logging.info(f"Подключение к листу {engineer}.")

    try:
        sheet = spreadsheet.worksheet(f"{engineer}")
    except gspread.exceptions.WorksheetNotFound:
        logging.info(f"Лист {engineer} не найден.")
        return None

    return sheet


def connect_to_engineer_ws_or_create(engineer: str) -> Worksheet:
    """
    Открывает лист проектировщика.
    Если лист не найден, создает его.
    """
    spreadsheet = connect_to_bonus_ws()

    logging.info(f"Подключение к листу {engineer}.")

    try:
        sheet = spreadsheet.worksheet(f"{engineer}")
    except gspread.exceptions.WorksheetNotFound:
        logging.info(f"Лист {engineer} не найден.")
        sheet = create_engineer_ws(spreadsheet, engineer)

    return sheet


def send_project_data_to_spreadsheet(df: DataFrame, engineer: str) -> Worksheet:
    """
    Отправляет данные с баллами в таблицу "Премирование".
    """
    logging.info("Отправка данных о проектах на лист проектировщика.")
    sheet = connect_to_engineer_ws_or_create(engineer)

    eng_small = df[
        [
            "Страна",
            "Наименование объекта",
            "Шифр (ИСП)",
            "Разработал",
            "Баллы",
            "Дата начала проекта",
            "Дата окончания проекта",
            "Дедлайн",
            "Автоматически определенная сложность",
        ]
    ]

    sheet.update([eng_small.columns.values.tolist()] + eng_small.values.tolist())

    color_overdue_deadline(eng_small, sheet)
    color_comp_correction(df, sheet)


def send_quarter_data_to_spreadsheet(df: DataFrame, engineer: str) -> Worksheet:
    """
    Отсылает данные о баллах, заработанных в каждом квартале
    в таблицу "Премирование".
    """
    logging.info("Отправка данных о баллах по кварталам на лист проектировщика.")
    sheet = connect_to_engineer_ws_or_create(engineer)

    sheet.update([df.columns.values.tolist()] + df.values.tolist(), range_name="L1:N200")


def send_results_data_ws(df: DataFrame) -> Worksheet:
    """Отправляет данные о средних баллах на лист "Настройки"."""
    worksheet = connect_to_settings_ws()

    logging.info('Отправка данных о средних баллах на лист "Настройки".')
    worksheet.update(
        [df.columns.values.tolist()] + df.values.tolist(), range_name="C1:E10"
    )

    return worksheet


def send_bonus_data_ws(engineer: str, df: DataFrame) -> Worksheet:
    """Отправляет данные о выполнении плана на лист проектировщика."""
    worksheet = connect_to_engineer_ws_or_create(engineer)

    logging.info(f"Отправка данных о выполнении плана проектировщика {engineer}.")
    worksheet.update(
        [df.columns.values.tolist()] + df.values.tolist(), range_name="N1:Q10"
    )

    return worksheet


def connect_to_attendance_sheet(month: str) -> Worksheet:
    """Подключается к табелю посещаемости офиса."""
    logging.info(f"Подключение к табелю посещаемости лист {month}.")
    try:
        spreadsheet: Spreadsheet = gc.open_by_url(ENDPOINT_ATTENDANCE_SHEET)
    except gspread.exceptions.SpreadsheetNotFound:
        logging.info("Табель посещаемости не найден.")
        raise gspread.exceptions.SpreadsheetNotFound(
            "Табель посещаемости не найден. Проверьте ссылку в .env файле "
            "и настройки доступа к таблице."
        )

    try:
        worksheet = spreadsheet.worksheet(month)
    except gspread.exceptions.WorksheetNotFound:
        logging.error(f"Лист {month} не найден.")
        return None

    return worksheet


def send_hours_data_ws(df: DataFrame) -> Worksheet:
    """Отправляет данные о рабочих часах на лист настроек."""
    worksheet = connect_to_settings_ws()

    logging.info('Отправка данных о рабочих часах на лист "Настройки".')
    worksheet.update(
        [df.columns.values.tolist()] + df.values.tolist(), range_name="G1:S30"
    )

    return worksheet


def delete_bonus_ws() -> None:
    """Удаляет текущую таблицу "Премирование"."""
    logging.info(f'Удаление таблицы "Премирование{dt.now().year}".')
    spreadsheet = gc.open(f"Премирование{dt.now().year}")
    gc.del_spreadsheet(spreadsheet.id)
    logging.info(f'Таблица "Премирование{dt.now().year}" удалена.')
