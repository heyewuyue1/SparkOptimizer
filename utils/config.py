import configparser


def read_config():
    config = configparser.ConfigParser()
    config.read('./config/config.cfg', encoding='utf-8')
    return config
