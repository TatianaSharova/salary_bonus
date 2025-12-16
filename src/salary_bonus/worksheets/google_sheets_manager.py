import time
from typing import Callable, Dict, Tuple

import gspread
from gspread.spreadsheet import Spreadsheet
from gspread.worksheet import Worksheet

from src.salary_bonus.config.defaults import COLOMNS_COUNT, ROWS_COUNT
from src.salary_bonus.config.environment import CREDS_PATH
from src.salary_bonus.logger import logging

gc = gspread.service_account(filename=CREDS_PATH)


class GoogleSheetsManager:
    """
    Класс-менеджер для подключений к Google Sheets.
    """

    def __init__(self, g_client: gspread.Client):
        """
        Инициализация GoogleSheetsManager.

        self.client: gspread.Client
            Клиент для доступа к Google Sheets APIо

        self._spreadsheets: Dict[str, Spreadsheet]
            Кеш открытых таблиц.

            Ключ:
                str - имя таблицы (title) или URL таблицы.
            Значение:
                gspread.Spreadsheet - объект открытой таблицы.

        self._worksheets: Dict[Tuple[str, str], Worksheet]
            Кеш открытых листов таблиц.

            Ключ:
                Tuple[str, str] - (spreadsheet_id, worksheet_title),
                где:
                    spreadsheet_id - уникальный идентификатор таблицы,
                    worksheet_title - название листа внутри таблицы.
            Значение:
                gspread.Worksheet - объект листа Google Sheets.
        """
        self.client: gspread.Client = g_client
        self._spreadsheets: Dict[str, Spreadsheet] = {}
        self._worksheets: Dict[Tuple[str, str], Worksheet] = {}

    def get_spreadsheet(self, title: str) -> Spreadsheet:
        logging.info(f'Открытие таблицы "{title}"')
        if title not in self._spreadsheets:
            try:
                self._spreadsheets[title] = self.client.open(title)
            except gspread.exceptions.SpreadsheetNotFound:
                logging.info(
                    f'Таблица "{title}" не найдена, или у бота нет доступа к ней'
                )
                raise
        return self._spreadsheets[title]

    def get_spreadsheet_by_url(self, url: str) -> Spreadsheet:
        if url not in self._spreadsheets:
            logging.info(f"Открытие таблицы по URL: {url}")
            try:
                self._spreadsheets[url] = self.client.open_by_url(url)
            except gspread.exceptions.SpreadsheetNotFound as err:
                logging.exception(
                    f"Таблица по ссылке {url} не найдена, "
                    f"или у бота нет доступа к ней: {err}"
                )
                raise
        return self._spreadsheets[url]

    def get_worksheet(
        self,
        spreadsheet: Spreadsheet,
        title: str,
    ) -> Worksheet | None:
        key = (spreadsheet.id, title)
        if key in self._worksheets:
            return self._worksheets[key]

        try:
            ws = spreadsheet.worksheet(title)
            self._worksheets[key] = ws
            return ws
        except gspread.exceptions.WorksheetNotFound:
            logging.info(f'Лист "{title}" не найден.')
            return None

    def get_all_worksheets(
        self,
        spreadsheet: Spreadsheet,
    ) -> list[Worksheet]:
        """Возвращает список всех листов (Worksheet) указанной таблицы."""
        cached_ws = [
            ws
            for (spreadsheet_id, _), ws in self._worksheets.items()
            if spreadsheet_id == spreadsheet.id
        ]

        if cached_ws:
            return cached_ws

        worksheets = spreadsheet.worksheets()

        for ws in worksheets:
            key = (spreadsheet.id, ws.title)
            self._worksheets[key] = ws

        return worksheets

    def get_or_create_spreadsheet(
        self,
        title: str,
        formatter: Callable[[Spreadsheet], None] | None = None,
        sleep_after: int = 0,
    ) -> Worksheet:
        try:
            spreadsheet = self.get_spreadsheet(title)
            return spreadsheet
        except gspread.exceptions.SpreadsheetNotFound:
            pass

        logging.info(f'Таблица "{title}" не найдена. ' f"Создание новой таблицы.")
        spreadsheet = gc.create(title)

        if formatter:
            formatter(spreadsheet)

        if sleep_after:
            logging.info(f"Ждем {sleep_after} секунд для продолжения работы")
            time.sleep(sleep_after)

        self._spreadsheets[title] = spreadsheet
        return spreadsheet

    def get_or_create_worksheet(
        self,
        spreadsheet: Spreadsheet,
        title: str,
        rows: int = ROWS_COUNT,
        cols: int = COLOMNS_COUNT,
        formatter: Callable[[Worksheet], None] | None = None,
        sleep_after: int = 0,
    ) -> Worksheet:
        ws = self.get_worksheet(spreadsheet, title)
        if ws:
            return ws

        logging.info(f'Создание листа "{title}".')
        ws = spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)

        if formatter:
            formatter(ws)
        logging.info(f'Лист "{title}" создан.')

        if sleep_after:
            logging.info(f"Ждем {sleep_after} секунд для продолжения работы")
            time.sleep(sleep_after)

        self._worksheets[(spreadsheet.id, title)] = ws
        return ws

    def invalidate(self) -> None:
        """
        Сброс всего кеша.
        """
        self._spreadsheets.clear()
        self._worksheets.clear()

    def invalidate_spreadsheet(self, spreadsheet: Spreadsheet | str) -> None:
        """
        Инвалидирует кеш конкретной таблицы.

        Args:
            spreadsheet: gspread.Spreadsheet или spreadsheet_id (str).
        """
        spreadsheet_id = spreadsheet if isinstance(spreadsheet, str) else spreadsheet.id

        self._spreadsheets = {
            key: value
            for key, value in self._spreadsheets.items()
            if value.id != spreadsheet_id
        }

        self._worksheets = {
            key: value
            for key, value in self._worksheets.items()
            if key[0] != spreadsheet_id
        }

    def invalidate_worksheet(
        self,
        spreadsheet: Spreadsheet | str,
        worksheet_title: str,
    ) -> None:
        """
        Инвалидирует кеш конкретного листа таблицы.
        """
        spreadsheet_id = spreadsheet if isinstance(spreadsheet, str) else spreadsheet.id

        self._worksheets.pop((spreadsheet_id, worksheet_title), None)

    def del_spreadsheet(self, title: str) -> None:
        """
        Удаление таблицы.
        """
        spreadsheet = self.get_spreadsheet(title)
        spreadsheet_id = spreadsheet.id

        self.client.del_spreadsheet(spreadsheet_id)
        logging.info(f"Удаление таблицы '{title}'")
        self.invalidate_spreadsheet(spreadsheet_id)


sheets_manager = GoogleSheetsManager(gc)
