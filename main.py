from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from pydantic import BaseModel
from s3_client import S3Client
from amplitude import process_week, parse_dates
import asyncio
import logging
from logging.handlers import RotatingFileHandler
import os
import json
import gc
from datetime import datetime
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv()

# Настройка logging: консоль + файл с rotation
DEBUG_MODE = os.getenv("DEBUG", "False").lower() == "true"
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG if DEBUG_MODE else logging.INFO)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)
logger.addHandler(console_handler)

# File handler with rotation
file_handler = RotatingFileHandler("app.log", maxBytes=10 * 1024 * 1024, backupCount=5)
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)
logger.addHandler(file_handler)

# Файл для persistence обработанных
PROCESSED_FILE = "processed_history.json"
if not os.path.exists(PROCESSED_FILE):
    with open(PROCESSED_FILE, "w") as f:
        json.dump([], f)  # Init как пустой list

app = FastAPI()

s3_client = S3Client()

# In-memory мониторинг (для runtime)
current_processing = {"date": None, "week": None, "processed": []}
errors = []


class DateRange(BaseModel):
    start_day: str
    end_day: str


@app.get("/current_date")
async def get_current_date():
    return {
        "current_date": current_processing["date"],
        "current_week": current_processing["week"],
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
    errors = []  # Reset errors for this run

    logger.info(f"Начало обработки периода: {days[0]} - {days[-1]} (дней: {len(days)})")

    # Группировка по неделям заранее
    weeks = defaultdict(list)
    for day in days:
        dt = datetime.strptime(day, "%Y%m%d")
        year, week, _ = dt.isocalendar()
        weeks[(year, week)].append(day)
    logger.info(f"Период разделён на {len(weeks)} недель")

    processed_weeks = []
    for (year, week), week_days in sorted(weeks.items()):  # По порядку
        current_processing["week"] = f"{year}_week_{week}"
        try:
            processed_id, no_data_days = await process_week(
                year, week, week_days, s3_client
            )
            processed_weeks.append(processed_id)
            current_processing["processed"].append(processed_id)

            # Persist после каждой недели
            entry = {
                "timestamp": datetime.now().isoformat(),
                "week": processed_id,
                "days": week_days,
                "no_data_days": no_data_days,
                "errors": False,
            }
            with open(PROCESSED_FILE, "r+") as f:
                history = json.load(f)
                history.append(entry)
                f.seek(0)
                json.dump(history, f, indent=4)
            logger.info(
                f"Обновлён processed_history.json: добавлена неделя {processed_id}"
            )
        except Exception as e:
            error_msg = f"Ошибка для недели {year}_{week}: {str(e)}"
            errors.append(error_msg)
            logger.error(error_msg)
            # Persist error entry
            entry = {
                "timestamp": datetime.now().isoformat(),
                "week": f"{year}_week_{week}",
                "days": week_days,
                "no_data_days": [],  # Нет, т.к. ошибка
                "errors": True,
            }
            with open(PROCESSED_FILE, "r+") as f:
                history = json.load(f)
                history.append(entry)
                f.seek(0)
                json.dump(history, f, indent=4)
        finally:
            current_processing["week"] = None
            gc.collect()

    logger.info(
        f"Обработка завершена. Обработано недель: {len(processed_weeks)}, Ошибок: {len(errors)}"
    )


@app.get("/logs")
async def get_logs():
    if os.path.exists("app.log"):
        return FileResponse("app.log", filename="app.log", media_type="text/plain")
    raise HTTPException(status_code=404, detail="Log file not found")


@app.get("/processed")
async def get_processed():
    if os.path.exists(PROCESSED_FILE):
        with open(PROCESSED_FILE, "r") as f:
            return json.load(f)
    return []


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
