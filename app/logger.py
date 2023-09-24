import logging
import os

log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bot_logs.txt')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file),
    ]
)
