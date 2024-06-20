"""
Алгоритм парсера:
1. Вытаскиваем структуру категорий из АПИ. Преобразуем в json и передаем на следующий этап;
2. Создаем excel-файл для сохранения главных категорий как отдельные листы в документе;
3. Используя рекурентный метод получаем данные из json о вложенных категориях;
4. В последней вложенности собираем данные из АПИ веб-сервиса с помощью асинхронных запросов и
валидируем используя pydantic модели;
5. Сохраняем excel-файл.
"""

import aiohttp
import asyncio
import requests
import xlsxwriter

from pydantic import BaseModel, Field
from typing import Optional

URL_MAIN_CATALOG = "https://static-basket-01.wbbasket.ru/vol0/data/main-menu-by-ru-v2.json"

HEADERS = {  # TODO: recommended to change for your machine
    "Accept": '*/*',
    "Accept-language": 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    "origin": 'https://www.wildberries.ru',
    "priority": 'u=1, i',
    "sec-ch-ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    "Referer": "https://www.wildberries.ru/",
    "sec-ch-ua-mobile": '?0',
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "sec-ch-ua-platform": "Windows",
    "sec-fetch-dest": 'empty',
    "sec-fetch-mode": 'cors',
    "sec-fetch-site": 'cross-site',
}


class Item(BaseModel):
    id: int
    name: str
    depth: Optional[int] = Field(default=99)
    parent: Optional[int] = None


class ListItems(BaseModel):
    name: Optional[str] = None
    items: Optional[list[Item]] = None


class Filter(BaseModel):
    filters: Optional[list[ListItems]]


class AsyncExcelSaver:
    def __init__(self, data, file_name="categories.xlsx"):
        self.workbook = xlsxwriter.Workbook(file_name)
        self.counter = 0
        asyncio.run(self._save_data_to_excel(data))

    async def _save_data_to_excel(self, data):
        """ Пункт 2 """
        tasks = []
        for main_category in data:
            worksheet = self.workbook.add_worksheet(main_category['name'])
            worksheet.write(0, 0, 'ID')
            worksheet.write(0, 1, 'Name')
            worksheet.write(0, 2, 'Depth')
            worksheet.write(0, 3, 'Parent')
            sheet = {'worksheet': worksheet, 'row': 1, 'col': 0}
            tasks.append(self._process_data(sheet, main_category, 1))

            print(f'Parsed: {main_category.get("name")}')

        await asyncio.gather(*tasks)
        self.workbook.close()
        print(f"Finished parsing. Amount of queries: {self.counter}")

    async def _process_data(self, sheet, category, depth):
        """ Пункт 3 """
        self._write_data_to_excel(sheet, category, depth)
        if 'childs' in category:
            tasks = [self._process_data(sheet, child, depth + 1) for child in category['childs']]
            await asyncio.gather(*tasks)
        else:
            await self._fetch_item_categories(sheet, category)

    async def _fetch_item_categories(self, sheet, category):
        """ Пункт 4 """
        url = 'https://catalog.wb.ru/catalog/{0}/v4/filters?appType=1&{1}&curr=rub&dest=-59202'.format(
            category.get('shard'), category.get('query')
        )
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            try:
                async with session.get(url) as response:
                    items = await response.json()
                    api_data = items.get('data', {})
                    api_data_filter = Filter(**api_data)
            except Exception:
                print("We have an unexpected response with:", category.get('url', 'have no URL'))
            else:
                self._write_item_category(sheet, api_data_filter, category.get('id', 0))
            finally:
                self.counter += 1

    def _write_item_category(self, sheet, data, parent):
        """ Пункт 4 """
        for filter_ in data.filters:
            if filter_.name == 'Категория':
                for item in filter_.items:
                    item.parent = parent
                    data_to_save = dict(item)
                    self._write_data_to_excel(sheet, data_to_save, item.depth)

    @staticmethod
    def _write_data_to_excel(sheet, category, depth):
        """ Пункт 5 """
        row = sheet['row']
        col = sheet['col']
        worksheet = sheet['worksheet']
        worksheet.write(row, col, category['id'])
        worksheet.write(row, col + 1, category['name'])
        worksheet.write(row, col + 2, depth)
        worksheet.write(row, col + 3, category.get('parent', 0))
        sheet['row'] += 1


class WildberriesParser:
    def __init__(self, url, headers):
        self.url = url
        self.headers = headers
        self.saver = AsyncExcelSaver

    def fetch_catalog(self):
        """ Пункт 1 """
        response = requests.get(url=self.url, headers=self.headers)
        return response.json()

    def parse_data(self, data):
        self.saver(data)


def run():
    parser = WildberriesParser(url=URL_MAIN_CATALOG, headers=HEADERS)
    data = parser.fetch_catalog()
    parser.parse_data(data)


if __name__ == '__main__':
    run()
