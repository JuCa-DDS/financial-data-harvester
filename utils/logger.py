import logging
import os

from tqdm import tqdm

class LoggerService:
    def __init__(self, name, log_file='scraper.log', level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        if not self.logger.handlers:
            os.makedirs('logs', exist_ok=True)
            log_path = os.path.join('logs', log_file)

            file_handler = logging.FileHandler(log_path, encoding='utf-8')
            formatter = logging.Formatter(
                '[%(asctime)s] %(levelname)s - %(name)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )

            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def get_logger(self):
        return self.logger
    