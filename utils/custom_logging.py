import sys
import logging
import datetime

def setup_custom_logger(name):
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler = logging.FileHandler(f'./logs/{datetime.datetime.now().strftime(r"%Y-%m-%d-%H-%M-%S")}.log', mode='w')
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)
    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(formatter)
    screen_handler.setLevel(logging.INFO)
    custom_logger = logging.getLogger(name)
    custom_logger.setLevel(logging.INFO)
    custom_logger.addHandler(handler)
    custom_logger.addHandler(screen_handler)
    return custom_logger


logger = setup_custom_logger('OPTIMIZER')
