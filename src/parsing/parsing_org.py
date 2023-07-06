import os
import logging
import re
import time

from tqdm import tqdm
import requests
import pandas as pd
import bs4.element
from bs4 import BeautifulSoup
from fake_useragent import UserAgent


class ParsingOrg:
    def __init__(
        self,
        path_df: str,
        path_output: str,
        path_dir_log: str,
        path_contract_problem: str,
        continue_parsing: bool = False,
        code_type: str = "code",
    ):
        """
        Метод парсит данные о заказчиках с сайта https://zakupki.gov.ru/
        """

        self.path_df = path_df
        self.path_output = path_output
        self.path_dir_log = path_dir_log
        self.continue_parsing = continue_parsing
        self.path_contract_problem = path_contract_problem
        self.code_type = code_type

        self.url_org = (
            f"https://zakupki.gov.ru/epz/organization/view/info.html?organization{code_type}="
        )

        self.list_columns_table = []
