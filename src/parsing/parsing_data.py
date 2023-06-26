import os
import logging
import datetime
import re
import time

import requests
import pandas as pd
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = "ALL:@SECLEVEL=1"


class ParsingDataContract:
    def __init__(self, path_df: str, path_output: str):
        """
        Метод парсит данные с сайта https://zakupki.gov.ru/
        """
        self.path_df = path_df
        self.path_output = path_output

    def initialize(self):
        self.df_input = pd.read_csv(self.path_df, sep="|", dtype="str")
        self.url_info = (
            "https://zakupki.gov.ru/epz/contract/contractCard/common-info.html?reestrNumber="
        )
        self.url_payment = """https://zakupki.gov.ru/epz/contract/contractCard/
        payment-info-and-target-of-order.html?reestrNumber=""".replace(
            "\n", ""
        )
        self.url_process = (
            "https://zakupki.gov.ru/epz/contract/contractCard/process-info.html?reestrNumber="
        )
        self.url_version = (
            "https://zakupki.gov.ru/epz/contract/contractCard/journal-version.html?reestrNumber="
        )
        self.url_document = (
            "https://zakupki.gov.ru/epz/contract/contractCard/document-info.html?reestrNumber="
        )
        self.url_event = (
            "https://zakupki.gov.ru/epz/contract/contractCard/event-journal.html?reestrNumber="
        )

    def init_logger(self) -> None:
        """
        Метод создает 2 логера, logger пишит данные в только в файл,
        logger_print пишит еще и в консоль
        """
        result_name = os.path.basename(self.path_name_result)
        file_log = os.path.join(self.path_dir_log, result_name)

        file_log = logging.FileHandler(f"{file_log}.log", mode="a")
        console_out = logging.StreamHandler()
        formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")
        file_log.setFormatter(formatter)

        # логер который выводит также данные в консоль
        self.logger_print = logging.getLogger(f"{__name__}_print")
        self.logger_print.setLevel(logging.INFO)
        self.logger_print.addHandler(file_log)
        self.logger_print.addHandler(console_out)

        # просто логер
        self.logger = logging.getLogger(f"{__name__}")
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(file_log)

    def get_page(self, url: str, id: str) -> object:
        for num_attempt in range(10):
            res = requests.get(
                f"{url}{id}",
                headers={"User-Agent": UserAgent().random},
                proxies=self.proxy,
            )
            if res.ok:
                return BeautifulSoup(res.text, "lxml")

    def get_data(self, res: object, adress_customer: str, inn_customer: str):
        """
        Метод извлекает данные из json файла, полученного в результате парсинга
        """

    # def run(self):
    #     self.initialize()
    #     self.init_logger()

    #     for index in range(len(self.path_df)):
    #         number_contract = self.path_df.loc["number_contract", index]
    #         adress_customer = self.path_df.loc["adress_customer", index]
    #         inn_customer = self.path_df.loc["inn_customer", index]
    #         res = self.get_page(url=self.url_info, id=number_contract)
