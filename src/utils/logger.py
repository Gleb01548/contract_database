import logging
from logging import Logger


def make_logger(path_for_file_log: str) -> tuple[Logger]:
    file_log = logging.FileHandler(path_for_file_log, mode="a")
    console_out = logging.StreamHandler()
    formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")
    file_log.setFormatter(formatter)

    # логер который выводит также данные в консоль
    logger_print = logging.getLogger(f"{__name__}_print")
    logger_print.setLevel(logging.INFO)
    logger_print.addHandler(file_log)
    logger_print.addHandler(console_out)

    # просто логер
    logger = logging.getLogger(f"{__name__}")
    logger.setLevel(logging.INFO)
    logger.addHandler(file_log)

    return logger_print, logger
