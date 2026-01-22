import logging
import sys

def get_logger(name: str) -> logging.Logger:
    """
    Returns a configured logger for Databricks environment.
    Ensures logs are visible in driver logs.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Check if handler already exists to avoid duplicate logs
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger
