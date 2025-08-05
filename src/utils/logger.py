import logging
import sys

def setup_logger(name: str = "TaxiAnalyticsLogger", level: int = logging.INFO) -> logging.Logger:
    """
    Sets up and configures a centralized logger for the application.

    This function is idempotent, meaning it will not add duplicate handlers
    if it's called multiple times on the same logger instance. This is
    crucial for environments with hot-reloading (like Uvicorn).

    Args:
        name (str, optional): The name of the logger. Defaults to "TaxiAnalyticsLogger".
        level (int, optional): The logging level (e.g., logging.INFO, logging.DEBUG).
                               Defaults to logging.INFO.

    Returns:
        logging.Logger: A configured logger instance.
    """
    # Get a logger instance by name
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # This is the key to prevent duplicate logs. We check if the logger
    # already has handlers configured. If it does, we don't add more.
    if not logger.handlers:
        # Create a handler to stream logs to standard output (the console)
        handler = logging.StreamHandler(sys.stdout)
        
        # Create a formatter to define the structure of the log messages
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Set the formatter for the handler
        handler.setFormatter(formatter)
        
        # Add the configured handler to the logger
        logger.addHandler(handler)
        
    return logger