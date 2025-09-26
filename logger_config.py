import logging
import logging.config
import os
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv

load_dotenv()

def get_log_level():
    """Получает уровень логирования из переменных окружения"""
    debug_value = os.getenv("DEBUG", "False").lower()
    if debug_value == "true":
        return logging.DEBUG
    else:
        return logging.INFO

def setup_logging():
    """Настраивает глобальную конфигурацию логирования"""
    level = get_log_level()
    
    # Создаем форматтер
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    # Создаем обработчики
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    
    file_handler = RotatingFileHandler(
        "app.log", 
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    
    # Настраиваем корневой логгер
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    
    # Убираем propagation для корневого логгера чтобы избежать дублирования
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

# Вызываем настройку при импорте модуля
setup_logging()

# Создаем логгер для этого модуля
logger = logging.getLogger(__name__)
logger.debug("Логгер сконфигурирован")