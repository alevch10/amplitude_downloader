import logger_config  # Должен быть первым импортом!
import logging
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from pydantic import BaseModel
from s3_client import S3Client
from amplitude import process_week, parse_dates
import asyncio
import json
import gc
from datetime import datetime
from dotenv import load_dotenv
from collections import defaultdict
import psutil
import tracemalloc
import os

logger = logging.getLogger(__name__)

load_dotenv()


PROCESSED_FILE = "processed_history.json"
if not os.path.exists(PROCESSED_FILE):
    with open(PROCESSED_FILE, "w") as f:
        json.dump([], f)

app = FastAPI()

s3_client = S3Client()

current_processing = {"date": None, "week": None, "processed": []}
errors = []

tracemalloc.start()

def log_memory_usage(step_name=""):
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    memory_mb = memory_info.rss / 1024 / 1024
    logger.info(f"💾 Память {step_name}: {memory_mb:.2f} MB")
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"🧠 Детали памяти {step_name}: RSS={memory_mb:.2f}MB")

def clear_memory():
    gc.collect()
    log_memory_usage("после очистки")

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

@app.get("/memory")
async def get_memory_info():
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    return {
        "memory_rss_mb": memory_info.rss / 1024 / 1024,
        "memory_vms_mb": memory_info.vms / 1024 / 1024,
    }

@app.post("/process")
async def process_dates(date_range: DateRange, background_tasks: BackgroundTasks):
    try:
        logger.info("📨 Получен запрос на обработку дат")
        logger.debug(f"🔍 Параметры запроса: start_day={date_range.start_day}, end_day={date_range.end_day}")
        
        days = parse_dates(date_range.start_day, date_range.end_day)
        background_tasks.add_task(run_processing, days)
        
        logger.info(f"🚀 Запущена фоновая обработка для {len(days)} дней")
        log_memory_usage("при старте обработки")
        
        return {"status": "started", "days": days}
    except ValueError as e:
        logger.error(f"❌ Ошибка парсинга дат: {e}")
        raise HTTPException(status_code=400, detail=str(e))

async def run_processing(days: list):
    global current_processing, errors
    current_processing["processed"] = []
    errors = []

    logger.info(f"🎬 НАЧАЛО ОБРАБОТКИ ПЕРИОДА: {days[0]} - {days[-1]} (дней: {len(days)})")
    logger.debug(f"📋 Дни для обработки: {days}")
    log_memory_usage("в начале run_processing")

    weeks = defaultdict(list)
    for day in days:
        dt = datetime.strptime(day, "%Y%m%d")
        year, week, _ = dt.isocalendar()
        weeks[(year, week)].append(day)
    
    logger.info(f"📈 Период разделён на {len(weeks)} недель")
    logger.debug(f"📊 Группировка по неделям: {dict(weeks)}")

    processed_weeks = []
    week_count = 0
    total_weeks = len(weeks)
    
    for (year, week), week_days in sorted(weeks.items()):
        week_count += 1
        current_processing["week"] = f"{year}_week_{week}"
        
        logger.info(f"🔷 [{week_count}/{total_weeks}] Обработка недели {year}_week_{week} ({len(week_days)} дней)")
        logger.debug(f"🔍 Дни недели {year}_{week}: {week_days}")
        log_memory_usage(f"перед неделей {year}_{week}")
        
        try:
            processed_id, no_data_days = await process_week(year, week, week_days, s3_client)
            processed_weeks.append(processed_id)
            current_processing["processed"].append(processed_id)

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
            
            logger.info(f"💾 Неделя {processed_id} сохранена в историю")
            logger.debug(f"📝 Запись в историю: {entry}")
            
            clear_memory()
            
            logger.info(f"✅ [{week_count}/{total_weeks}] Неделя {year}_week_{week} завершена успешно")
            
        except Exception as e:
            error_msg = f"❌ Ошибка для недели {year}_{week}: {str(e)}"
            errors.append(error_msg)
            logger.error(error_msg)
            
            entry = {
                "timestamp": datetime.now().isoformat(),
                "week": f"{year}_week_{week}",
                "days": week_days,
                "no_data_days": [],
                "errors": True,
            }
            
            with open(PROCESSED_FILE, "r+") as f:
                history = json.load(f)
                history.append(entry)
                f.seek(0)
                json.dump(history, f, indent=4)
            
            clear_memory()
            logger.error(f"❌ [{week_count}/{total_weeks}] Неделя {year}_week_{week} завершена с ошибкой")
            
        finally:
            current_processing["week"] = None

    logger.info(f"🎉 ВСЯ ОБРАБОТКА ЗАВЕРШЕНA. Обработано недель: {len(processed_weeks)}, Ошибок: {len(errors)}")
    log_memory_usage("в конце обработки")

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

@app.get("/status")
async def get_status():
    return {
        "current_week": current_processing["week"],
        "processed_weeks": current_processing["processed"],
        "errors_count": len(errors),
        "memory_info": await get_memory_info()
    }

if __name__ == "__main__":
    import uvicorn
    logger.info("🟢 Сервер запускается на http://0.0.0.0:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)