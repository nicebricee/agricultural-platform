"""
Logging configuration for the Agricultural Data Platform.
"""

import sys
import logging
from loguru import logger
from pathlib import Path
from .config import settings


def setup_logging():
    """Configure application logging using loguru."""
    
    # Remove default logger
    logger.remove()
    
    # Configure log format based on settings
    if settings.log_format == "json":
        log_format = "{message}"  # For JSON, just use serialization
    else:
        log_format = (
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{module}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        )
    
    # Add console handler
    logger.add(
        sys.stdout,
        format=log_format,
        level=settings.log_level,
        colorize=settings.log_format != "json",
        serialize=settings.log_format == "json"
    )
    
    # Add file handler for logging - single file for visibility
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Single comprehensive log file for all backend processes
    logger.add(
        log_dir / "backend.log",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} | {message}",
        level="DEBUG",  # Capture everything for full visibility
        rotation=None,  # No rotation - continuous log
        retention=None,  # Keep all logs
        compression=None,  # No compression for real-time viewing
        serialize=False,  # Human-readable format
        backtrace=True,  # Include stack traces
        diagnose=True   # Include variable values in errors
    )
    
    # Configure standard logging to use loguru
    class InterceptHandler(logging.Handler):
        def emit(self, record):
            try:
                level = logger.level(record.levelname).name
            except ValueError:
                level = record.levelno
            
            frame, depth = logging.currentframe(), 2
            while frame.f_code.co_filename == logging.__file__:
                frame = frame.f_back
                depth += 1
            
            logger.opt(depth=depth, exception=record.exc_info).log(
                level, record.getMessage()
            )
    
    # Intercept standard logging
    logging.basicConfig(handlers=[InterceptHandler()], level=0)
    
    # Intercept uvicorn logs
    for logger_name in ["uvicorn", "uvicorn.error", "uvicorn.access"]:
        logging.getLogger(logger_name).handlers = [InterceptHandler()]
    
    return logger


# Initialize logger
app_logger = setup_logging()