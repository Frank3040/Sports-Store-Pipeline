import logging
import sys
from pathlib import Path

def setup_logger(name: str, log_file: Path | None = None, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:  # Avoid duplicate handlers
        return logger

    # Formatter for structured logs (timestamp, level, name, message)
    formatter = logging.Formatter('%(asctime)s - [%(name)s] - %(levelname)s - %(message)s')

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler if specified
    if log_file:
        # Ensure parent directory exists
        log_file.parent.mkdir(parents=True, exist_ok=True)

        # Touch the file if it doesn't exist
        if not log_file.exists():
            log_file.touch()

        file_handler = logging.FileHandler(log_file, mode='a')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
