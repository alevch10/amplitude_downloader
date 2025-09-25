from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from s3_client import S3Client
from amplitude import download_and_process_day, parse_dates
import asyncio
import logging
import os
from dotenv import load_dotenv

load_dotenv()

# Настройка logging
DEBUG_MODE = os.getenv("DEBUG", "False").lower() == "true"
logging.basicConfig(
    level=logging.DEBUG if DEBUG_MODE else logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],  # Вывод в консоль
)
logger = logging.getLogger(__name__)

app = FastAPI()

s3_client = S3Client()

# In-memory мониторинг
current_processing = {"date": None, "processed": []}
errors = []


class DateRange(BaseModel):
    start_day: str
    end_day: str


@app.get("/current_date")
async def get_current_date():
    return {
        "current": current_processing["date"],
        "processed": current_processing["processed"],
    }


@app.get("/errors")
async def get_errors():
    return {"errors": errors}


@app.post("/process")
async def process_dates(date_range: DateRange, background_tasks: BackgroundTasks):
    try:
        days = parse_dates(date_range.start_day, date_range.end_day)
        background_tasks.add_task(run_processing, days)
        logger.info(f"Запущена обработка для дней: {days}")
        return {"status": "started", "days": days}
    except ValueError as e:
        logger.error(f"Ошибка парсинга дат: {e}")
        raise HTTPException(status_code=400, detail=str(e))


async def run_processing(days: list):
    global current_processing, errors
    current_processing["processed"] = []
    semaphore = asyncio.Semaphore(
        7
    )  # Лимит на 2 параллельных дня для избежания переполнения
    logger.info(f"Начата асинхронная обработка {len(days)} дней")

    async def process_with_sem(day):
        async with semaphore:
            current_processing["date"] = day
            logger.debug(f"Начата обработка дня: {day}")
            try:
                processed_day = await download_and_process_day(day, s3_client)
                current_processing["processed"].append(processed_day)
                logger.debug(f"Успешно обработан день: {day}")
            except Exception as e:
                error_msg = f"Ошибка для {day}: {e}"
                errors.append(error_msg)
                logger.error(error_msg)
            finally:
                if current_processing["date"] == day:
                    current_processing["date"] = None
                logger.debug(f"Завершена обработка дня: {day}")

    await asyncio.gather(*(process_with_sem(day) for day in days))
    logger.info(
        f"Обработка всех дней завершена. Обработано: {current_processing['processed']}, Ошибок: {len(errors)}"
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
