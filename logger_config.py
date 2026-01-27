import logging
import os
import time
from datetime import datetime
from functools import wraps
from logging.handlers import RotatingFileHandler

# ===============================
# Project and Log Directory Setup
# ===============================
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# ===============================
# Date-based Log File Name
# Format: DD.MM.YYYY.log
# ===============================
today_str = datetime.now().strftime("%d.%m.%Y")
log_filename = f"{today_str}.log"
log_path = os.path.join(LOG_DIR, log_filename)

# ===============================
# Custom Rotating Handler
# Renames: file.log.1 → file.1.log
# ===============================
class DateSizeRotatingHandler(RotatingFileHandler):
    def rotation_filename(self, default_name):
        """
        Convert:
        10.01.2026.log.1 → 10.01.2026.1.log
        """
        if default_name.count(".") >= 2:
            base, ext, index = default_name.rsplit(".", 2)
            return f"{base}.{index}.{ext}"
        return default_name

# ===============================
# Logger Configuration
# ===============================
logger = logging.getLogger("real_estate_file_handler")
logger.setLevel(logging.DEBUG)
logger.propagate = False

# ===============================
# Custom Formatter
# ===============================
class FuncTimestampFormatter(logging.Formatter):
    def format(self, record):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        func_name = record.funcName
        original_msg = record.msg

        record.msg = f"{timestamp} | {func_name} | {original_msg}"
        formatted = super().format(record)
        record.msg = original_msg

        return formatted

formatter = FuncTimestampFormatter("%(msg)s")

# ===============================
# File Handler (10MB rotation)
# ===============================
file_handler = DateSizeRotatingHandler(
    log_path,
    maxBytes=10 * 1024 * 1024,  # 10 MB
    backupCount=7,
    encoding="utf-8"
)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

# ===============================
# Console Handler
# ===============================
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# ===============================
# Attach Handlers (avoid duplicates)
# ===============================
if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

# ===============================
# Function Timing Decorator
# ===============================
def log_time(func):
    """
    Decorator to log function start, end, and execution time.
    Usage:
        @log_time
        def your_function():
            ...
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.info("Started")
        start_time = time.time()
        try:
            return func(*args, **kwargs)
        finally:
            duration = time.time() - start_time
            logger.info(f"Completed | Duration: {duration:.2f} seconds")
    return wrapper
