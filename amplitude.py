import zipfile
import gzip
import io
import os
from datetime import datetime, timedelta
import asyncio
import aiohttp
from dotenv import load_dotenv
import logging
import gc

load_dotenv()

AMPLITUDE_CLIENT_ID = os.getenv("AMPLITUDE_CLIENT_ID")
AMPLITUDE_SECRET_ID = os.getenv("AMPLITUDE_SECRET_ID")

# Logger (тот же, что в main.py)
logger = logging.getLogger(__name__)


async def download_day(day: str):
    """Скачивает и распаковывает данные за день in-memory, возвращает список строк (events) из всех json файлов дня."""
    start = f"{day}T00"
    end = f"{day}T23"
    url = f"https://amplitude.com/api/2/export?start={start}&end={end}"

    auth = aiohttp.BasicAuth(AMPLITUDE_CLIENT_ID, AMPLITUDE_SECRET_ID)

    logger.debug(f"Скачивание данных для {day} с URL: {url}")
    async with aiohttp.ClientSession() as session:
        async with session.get(url, auth=auth) as response:
            if response.status == 404:
                error_text = await response.text()
                if "Raw data files were not found." in error_text:
                    logger.info(
                        f"Нет данных для дня {day} (404: Raw data files not found)"
                    )
                    return []  # Пустой список, не ошибка
                else:
                    raise ValueError(
                        f"Ошибка Amplitude: {response.status} - {error_text}"
                    )
            if response.status != 200:
                raise ValueError(
                    f"Ошибка Amplitude: {response.status} - {await response.text()}"
                )
            zip_content = await response.read()
    logger.debug(f"Данные для {day} скачаны (размер: {len(zip_content)} байт)")

    # Распаковка zip in-memory
    with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_ref:
        # Предполагаем, что внутри одна папка с .gz файлами
        gz_files = [name for name in zip_ref.namelist() if name.endswith(".gz")]
        if not gz_files:
            raise ValueError("Нет .gz файлов в zip")

        all_lines = []  # Список всех строк (events) за день
        for gz_name in gz_files:
            with zip_ref.open(gz_name) as gz_file:
                gz_content = gz_file.read()
                json_content = gzip.decompress(gz_content)
                # Декодируем в строки (utf-8), разбиваем на lines
                lines = json_content.decode("utf-8").splitlines()
                all_lines.extend(lines)
                logger.debug(f"Распакован {gz_name}: {len(lines)} строк")

    return all_lines  # Возвращаем все events дня как list[str]


async def process_week(
    year: int, week: int, week_days: list, s3_client, semaphore_value: int = 7
):
    """Обрабатывает одну неделю: скачивает дни parallel, агрегирует, zip, upload."""
    logger.info(f"Начата обработка недели {year}_week_{week} (дней: {len(week_days)})")
    semaphore = asyncio.Semaphore(semaphore_value)

    week_data = {}  # {day: list_of_lines}
    no_data_days = []  # Новый список для дней без данных

    async def process_day(day):
        async with semaphore:
            logger.debug(f"Начата обработка дня {day} в неделе {year}_{week}")
            try:
                lines = await download_day(day)
                if lines:  # Если не пусто
                    week_data[day] = lines
                    logger.debug(f"Успешно обработан день {day} ({len(lines)} строк)")
                else:
                    no_data_days.append(day)  # Добавляем в no_data
            except Exception as e:
                logger.error(f"Ошибка для дня {day} в неделе {year}_{week}: {str(e)}")
                raise  # Передаём вверх для errors

    # Parallel скачивание дней недели
    await asyncio.gather(*(process_day(day) for day in week_days))

    # Агрегация lines
    week_lines = []
    for day in sorted(week_days):
        week_lines.extend(week_data.get(day, []))
    del week_data
    gc.collect()

    # Склеиваем in-memory
    ndjson_io = io.StringIO()
    for line in week_lines:
        ndjson_io.write(line + "\n")
    ndjson_content = ndjson_io.getvalue().encode("utf-8")
    ndjson_io.close()
    del week_lines
    gc.collect()

    # Zip in-memory
    zip_io = io.BytesIO()
    with zipfile.ZipFile(zip_io, "w", zipfile.ZIP_DEFLATED) as new_zip:
        new_zip.writestr(f"{year}_week_{week}.ndjson", ndjson_content)
    zip_io.seek(0)

    # Upload
    s3_object_name = f"amplitude/{year}_week_{week}.zip"
    logger.debug(f"Загрузка в S3: {s3_object_name}")
    s3_client.client.put_object(
        Bucket=s3_client.bucket, Key=s3_object_name, Body=zip_io.getvalue()
    )
    logger.info(f"Успешная загрузка в S3 для недели {year}_{week}")

    del zip_io, ndjson_content
    gc.collect()
    logger.info(f"Завершена неделя {year}_week_{week}. Дней без данных: {no_data_days}")
    return f"{year}_week_{week}", no_data_days


def parse_dates(start_day: str, end_day: str):
    """Парсит даты и генерирует список дней в формате YYYYMMDD."""
    start_dt = datetime.strptime(start_day[:8], "%Y%m%d")
    end_dt = datetime.strptime(end_day[:8], "%Y%m%d")
    days = []
    current = start_dt
    while current <= end_dt:
        days.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    logger.debug(f"Сгенерированы дни: {days}")
    return days
