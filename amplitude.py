import zipfile
import gzip
import shutil
import tempfile
import os
from datetime import datetime, timedelta
import aiohttp
from dotenv import load_dotenv
import logging

load_dotenv()

AMPLITUDE_CLIENT_ID = os.getenv("AMPLITUDE_CLIENT_ID")
AMPLITUDE_SECRET_ID = os.getenv("AMPLITUDE_SECRET_ID")

# Logger (тот же, что в main.py)
logger = logging.getLogger(__name__)


async def download_and_process_day(day: str, s3_client):
    """Асинхронная обработка одного дня."""
    start = f"{day}T00"
    end = f"{day}T23"
    url = f"https://amplitude.com/api/2/export?start={start}&end={end}"

    auth = aiohttp.BasicAuth(AMPLITUDE_CLIENT_ID, AMPLITUDE_SECRET_ID)

    logger.debug(f"Скачивание данных для {day} с URL: {url}")
    async with aiohttp.ClientSession() as session:
        async with session.get(url, auth=auth) as response:
            if response.status != 200:
                raise ValueError(
                    f"Ошибка Amplitude: {response.status} - {await response.text()}"
                )
            zip_content = await response.read()
    logger.debug(f"Данные для {day} скачаны (размер: {len(zip_content)} байт)")

    with tempfile.TemporaryDirectory() as tmp_dir:
        zip_path = os.path.join(tmp_dir, "amplitude.zip")
        with open(zip_path, "wb") as f:
            f.write(zip_content)
        logger.debug(f"Zip сохранён во временной папке: {zip_path}")

        # Распаковка zip
        logger.debug(f"Распаковка zip для {day}")
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(tmp_dir)

        # Найти внутреннюю папку (предполагаем, что одна)
        inner_dir = next(
            (
                os.path.join(tmp_dir, d)
                for d in os.listdir(tmp_dir)
                if os.path.isdir(os.path.join(tmp_dir, d))
            ),
            None,
        )
        if not inner_dir:
            raise ValueError("Нет внутренней папки в zip")
        logger.debug(f"Внутренняя папка найдена: {inner_dir}")

        # Распаковка всех .gz в .json
        logger.debug(f"Распаковка .gz файлов в {inner_dir}")
        gz_files = [f for f in os.listdir(inner_dir) if f.endswith(".gz")]
        for gz_file in gz_files:
            gz_path = os.path.join(inner_dir, gz_file)
            # Умный replace: если .json.gz, replace '.json.gz' на '.json', иначе '.gz' на '.json'
            if gz_file.endswith(".json.gz"):
                json_path = gz_path.replace(".json.gz", ".json")
            else:
                json_path = gz_path.replace(".gz", ".json")
            with gzip.open(gz_path, "rb") as f_in:
                with open(json_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(gz_path)
            logger.debug(
                f"Распакован и удалён: {gz_file} → {os.path.basename(json_path)}"
            )

        # Запаковка .json файлов напрямую в новый zip (без вложенной папки)
        new_zip_path = os.path.join(tmp_dir, f"{day}.zip")
        logger.debug(f"Запаковка json в новый zip: {new_zip_path}")
        with zipfile.ZipFile(new_zip_path, "w", zipfile.ZIP_DEFLATED) as new_zip:
            # Только файлы из inner_dir, без поддиректорий (предполагаем, что все в корне)
            for file in os.listdir(inner_dir):
                if file.endswith(".json"):
                    file_path = os.path.join(inner_dir, file)
                    new_zip.write(file_path, arcname=file)  # Прямо в root zip

        # Загрузка в S3
        s3_object_name = f"amplitude/{day}.zip"
        logger.debug(f"Загрузка в S3: {s3_object_name}")
        s3_client.upload_file(new_zip_path, s3_object_name)
        logger.debug(f"Успешная загрузка в S3 для {day}")

    return day  # Для мониторинга


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
