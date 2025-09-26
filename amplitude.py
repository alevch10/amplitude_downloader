import zipfile
import gzip
import io
import os
from datetime import datetime, timedelta
import asyncio
import aiohttp
import logging
import gc

# Логгер создается после всех импортов
logger = logging.getLogger(__name__)

AMPLITUDE_CLIENT_ID = os.getenv("AMPLITUDE_CLIENT_ID")
AMPLITUDE_SECRET_ID = os.getenv("AMPLITUDE_SECRET_ID")

async def download_day(day: str):
    """Скачивает и распаковывает данные за день in-memory."""
    start = f"{day}T00"
    end = f"{day}T23"
    url = f"https://amplitude.com/api/2/export?start={start}&end={end}"

    auth = aiohttp.BasicAuth(AMPLITUDE_CLIENT_ID, AMPLITUDE_SECRET_ID)

    logger.info(f"🔄 Начало скачивания дня {day}")
    logger.debug(f"📤 Запрос к Amplitude для дня {day}: {url}")
    
    async with aiohttp.ClientSession() as session:
        logger.debug(f"🚀 Отправка запроса к Amplitude для дня {day}")
        async with session.get(url, auth=auth) as response:
            logger.debug(f"📥 Получен ответ от Amplitude для дня {day}: статус {response.status}")
            
            if response.status == 404:
                error_text = await response.text()
                if "Raw data files were not found." in error_text:
                    logger.info(f"❌ Нет данных для дня {day}")
                    return []
                else:
                    raise ValueError(f"Ошибка Amplitude: {response.status} - {error_text}")
            if response.status != 200:
                raise ValueError(f"Ошибка Amplitude: {response.status} - {await response.text()}")
                
            zip_content = await response.read()
    
    logger.info(f"✅ День {day} скачан успешно, размер: {len(zip_content) / 1024 / 1024:.2f} MB")

    logger.debug(f"🗜️ Начало распаковки ZIP для дня {day}")
    with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_ref:
        gz_files = [name for name in zip_ref.namelist() if name.endswith(".gz")]
        logger.debug(f"📁 Найдены файлы в ZIP: {gz_files}")
        
        if not gz_files:
            raise ValueError("Нет .gz файлов в zip")

        all_lines = []
        for gz_name in gz_files:
            logger.debug(f"🔓 Распаковка файла: {gz_name}")
            with zip_ref.open(gz_name) as gz_file:
                gz_content = gz_file.read()
                json_content = gzip.decompress(gz_content)
                lines = json_content.decode("utf-8").splitlines()
                all_lines.extend(lines)
                logger.debug(f"✅ Распакован {gz_name}: {len(lines)} строк")

    logger.info(f"✅ День {day} распакован, событий: {len(all_lines)}")
    return all_lines


async def process_week(year: int, week: int, week_days: list, s3_client, semaphore_value: int = 7):
    """Обрабатывает одну неделю с улучшенным управлением памятью."""
    logger.info(f"🚀 НАЧАЛО ОБРАБОТКИ НЕДЕЛИ {year}_week_{week} (дней: {len(week_days)})")
    semaphore = asyncio.Semaphore(semaphore_value)

    week_data = {}
    no_data_days = []

    async def process_day(day):
        async with semaphore:
            logger.info(f"📥 Обработка дня {day} в неделе {year}_{week}")
            try:
                lines = await download_day(day)
                if lines:
                    week_data[day] = lines
                    logger.info(f"✅ День {day} обработан успешно, событий: {len(lines)}")
                    
                    del lines
                    gc.collect()
                else:
                    no_data_days.append(day)
                    logger.info(f"➖ День {day} без данных")
            except Exception as e:
                logger.error(f"❌ Ошибка для дня {day} в неделе {year}_{week}: {str(e)}")
                raise

    logger.info(f"⏳ Параллельное скачивание {len(week_days)} дней недели {year}_{week}")
    await asyncio.gather(*(process_day(day) for day in week_days))
    logger.info(f"✅ Все дни недели {year}_{week} скачаны")

    logger.info(f"🧩 Сборка данных недели {year}_{week}")
    ndjson_io = io.StringIO()
    total_lines = 0
    
    for day in sorted(week_days):
        if day in week_data:
            lines = week_data[day]
            for line in lines:
                ndjson_io.write(line + "\n")
            total_lines += len(lines)
            del week_data[day]
            gc.collect()
    
    ndjson_content = ndjson_io.getvalue().encode("utf-8")
    ndjson_io.close()
    
    del week_data
    gc.collect()

    logger.info(f"✅ Данные недели {year}_{week} собраны, всего событий: {total_lines}")

    logger.info(f"🗜️ Создание ZIP-архива для недели {year}_{week}")
    zip_io = io.BytesIO()
    with zipfile.ZipFile(zip_io, "w", zipfile.ZIP_DEFLATED) as new_zip:
        new_zip.writestr(f"{year}_week_{week}.ndjson", ndjson_content)
    zip_io.seek(0)

    zip_size = len(zip_io.getvalue()) / 1024 / 1024
    logger.info(f"✅ ZIP создан, размер: {zip_size:.2f} MB")

    s3_object_name = f"amplitude/{year}_week_{week}.zip"
    logger.info(f"☁️ Начало загрузки в S3: {s3_object_name}")
    logger.debug(f"📤 Отправка в S3: {s3_object_name}, размер: {zip_size:.2f} MB")
    
    s3_client.client.put_object(
        Bucket=s3_client.bucket, Key=s3_object_name, Body=zip_io.getvalue()
    )
    
    logger.info(f"✅ Успешная загрузка в S3 для недели {year}_{week}")
    logger.debug(f"📥 Загрузка в S3 завершена: {s3_object_name}")

    del ndjson_content, zip_io
    gc.collect()
    
    logger.info(f"🎉 ЗАВЕРШЕНА НЕДЕЛЯ {year}_week_{week}. Дней без данных: {no_data_days}")
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
    logger.info(f"📅 Сгенерирован период: {len(days)} дней с {days[0]} по {days[-1]}")
    return days