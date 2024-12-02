import logging
import os
import threading
import platform

def setup_logging(config):
    # Create logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Capture all levels of logs

    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s %(name)s:%(lineno)d [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Color codes for different log levels
    class ColorFormatter(logging.Formatter):
        # Standard ANSI escape sequences
        COLORS = {
            'WARNING': '\033[33m',    # yellow
            'INFO': '\033[32m',       # green
            'DEBUG': '\033[34m',      # blue
            'CRITICAL': '\033[31m',   # red
            'ERROR': '\033[31m',      # red
        }

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            # Check if we're on Windows and if we need to enable ANSI support
            if platform.system() == 'Windows':
                import ctypes
                kernel32 = ctypes.windll.kernel32
                # Enable ANSI support in Windows console
                kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)

        def format(self, record):
            if record.levelname in self.COLORS:
                # Use ANSI escape sequences for color
                record.levelname = f"{self.COLORS[record.levelname]}{record.levelname}\033[0m"
            return super().format(record)

    console_formatter = ColorFormatter(
        '%(levelname)s: %(message)s'
    )

    # File handler - detailed logging
    file_handler = logging.FileHandler(config.LOG_FILE)
    file_handler.setLevel(logging.DEBUG)  # Log everything to file
    file_handler.setFormatter(detailed_formatter)

    # Console handler - only important messages
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # Only INFO and above
    console_handler.setFormatter(console_formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
