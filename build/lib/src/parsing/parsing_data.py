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


class ParsingDataContract:
    def __init__(
        self,
        path_df: str,
        path_output: str,
        path_dir_log: str,
        path_contract_problem: str,
        continue_parsing: bool = False,
    ) -> None:
        """
        Метод парсит данные с сайта https://zakupki.gov.ru/
        """
        self.path_df = path_df
        self.path_output = path_output
        self.path_dir_log = path_dir_log
        self.continue_parsing = continue_parsing
        self.path_contract_problem = path_contract_problem

        self.url_info = (
            "https://zakupki.gov.ru/epz/contract/contractCard/common-info.html?reestrNumber="
        )
        self.url_payment = "https://zakupki.gov.ru/epz/contract/contractCard/"
        self.url_payment += "payment-info-and-target-of-order.html?reestrNumber="

        self.list_columns_table = [
            "number_contract",
            "adress_customer",
            "full_name_customer",
            "short_name_customer",
            "unique_site_code",
            "unique_site_id",
            "id_customer",
            "inn_customer",
            "kpp_customer",
            "code_form_org",
            "okpo_code",
            "municipal_code",
            "budget_name",
            "extrabudget_name",
            "budget_level",
            "contract_status",
            "notice",
            "ikz_code",
            "id_contract_electronic",
            "unique_number_plan",
            "method_determinig_supplier",
            "date_summarizing",
            "date_posting",
            "grouds_single_supplier",
            "document_details",
            "info_support",
            "find_date_contract",
            "date_performance",
            "date_contract_registry",
            "date_update_registry",
            "date_start_performance",
            "date_end_performance",
            "contract_item",
            "contract_price",
            "contract_price_nds",
            "prepayment_amount",
            "performance_security",
            "size_performance_quality",
            "warranty_period",
            "place_performance",
            "full_name_supplier",
            "inn_supplier",
            "kpp_supplier",
            "code_okpo_supplier",
            "date_registration_supplier",
            "country_supplier",
            "code_country_supplier",
            "adress_supplier",
            "postal_adress_supplier",
            "contact",
            "status_supplier",
            "kbk",
        ]

    def initialize(self):
        dir_name = os.path.basename(os.path.dirname(self.path_df))
        self.path_output = os.path.join(
            self.path_output, dir_name, os.path.basename(self.path_df)
        )

        # убрать этот код и добавить потом в общий
        if not os.path.exists(os.path.dirname(self.path_output)):
            os.makedirs(os.path.dirname(self.path_output))

        if not os.path.exists(self.path_output) or not self.continue_parsing:
            pd.DataFrame(columns=self.list_columns_table).to_csv(
                self.path_output, index=False, sep="|"
            )

        self.df_input = pd.read_csv(self.path_df, sep="|", dtype="str")

        self.path_contract_problem = os.path.join(
            self.path_contract_problem, dir_name, os.path.basename(self.path_df)
        )

        if not os.path.exists(os.path.dirname(self.path_contract_problem)):
            os.makedirs(os.path.dirname(self.path_contract_problem))

        if not os.path.exists(self.path_contract_problem) or not self.continue_parsing:
            columns_prob = list(self.df_input.columns)
            columns_prob.append("сommentary")
            pd.DataFrame(columns=columns_prob).to_csv(
                self.path_contract_problem, index=False, sep="|"
            )
        self.df_problem = pd.read_csv(self.path_contract_problem, sep="|", dtype="str")

        self.init_logger()

        if self.continue_parsing:
            self.make_continue_parsing()

    def make_continue_parsing(self):
        df_output = pd.read_csv(self.path_output, sep="|", dtype="str")
        last_number_contract = df_output["number_contract"].iloc[-1]

        last_index = self.df_input["number_contract"].to_list().index(last_number_contract)
        self.df_input = self.df_input[self.df_input.index >= last_index].reset_index(drop=True)
        df_output.iloc[:-1].to_csv(self.path_output, sep="|", index=False)

    def init_logger(self) -> None:
        """
        Метод создает 2 логера, logger пишит данные в только в файл,
        logger_print пишит еще и в консоль
        """
        dir_name = os.path.basename(os.path.dirname(self.path_output))
        self.file_name = os.path.basename(self.path_output).removesuffix(".csv")
        file_log = os.path.join(
            self.path_dir_log,
            f"{dir_name}_{self.file_name}",
        )

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

    def get_page(self, url: str) -> BeautifulSoup:
        """
        Метод считывает заданную страницу,
        а потом преобразует в lxml
        """
        for i in range(1, 11):
            try:
                res = requests.get(url, headers={"User-Agent": UserAgent().random})
            except requests.exceptions.ConnectionError:
                if self.check_internet_every_n_sec(120, 10):
                    continue

            if res is not None and res.ok:
                return BeautifulSoup(res.text, "lxml")
            elif i == 3 or i == 7:
                self.check_maintenance()

    def remove_bad_symbols(self, string: str) -> str:
        string = re.sub("\n|\||\xa0", "", string.strip())
        return " ".join(string.split())

    def check_internet(self):
        google = requests.get(
            "https://www.google.ru/", headers={"User-Agent": UserAgent().random}
        ).ok
        google_com = requests.get(
            "https://www.google.com/", headers={"User-Agent": UserAgent().random}
        ).ok
        yandex = requests.get("https://ya.ru/", headers={"User-Agent": UserAgent().random}).ok
        mail = requests.get("https://mail.ru/", headers={"User-Agent": UserAgent().random}).ok
        return any([google, google_com, yandex, mail])

    def check_internet_every_n_sec(self, times: int, seconds: int) -> bool:
        self.logger.info("Проверка интернет-соединения")
        for _ in range(times):
            time.sleep(seconds)
            try:
                if self.check_internet():
                    return True
            except requests.exceptions.ConnectionError:
                pass

        warn = "НЕТ ИНТЕРНЕТА"
        self.logger.warning(warn)
        raise IOError(warn)

    def check_maintenance(self) -> None:
        self.logger.info("Проверка сайта на технические работы")
        list_number_check = [
            "0363200013140000022",
            "0828300088140000016",
            "0853300009414000001",
            "0711200006614000007",
            "0349200015514000201",
            "0347200000814000015",
        ]
        for number_contract in list_number_check:
            res = requests.get(
                f"{self.url_info}{number_contract}", headers={"User-Agent": UserAgent().random}
            )
            if not res.ok:
                continue
            else:
                return None
        warn = "НА САЙТЕ ТЕХНИЧЕСКИЕ РАБОТЫ"
        self.logger.warning(warn)
        raise IOError(warn)

    # ИНФОРМАЦИЯ О ЗАКАЗЧИКЕ
    def find_full_name_customer(self, soup: BeautifulSoup) -> str:
        """
        Полное наименование заказчика
        """
        try:
            soup = soup.find("span", string="Полное наименование заказчика")
            castomer = soup.parent.find("a", target="_blank")
            castomer = castomer.get_text()
            return self.remove_bad_symbols(castomer)
        except AttributeError:
            self.logger.info("Не выделено Полное наименование заказчика")
            return None

    def find_short_name_customer(self, soup: BeautifulSoup) -> str:
        """
        сокращенное наименование заказчика
        """
        try:
            soup = soup.find("span", string="Сокращенное наименование заказчика")
            castomer = soup.parent.find("span", class_="section__info")
            castomer = castomer.get_text()
            return self.remove_bad_symbols(castomer)
        except AttributeError:
            self.logger.info("Не выделено Сокращенное наименование заказчика")
            return None

    def find_unique_account_number(self, soup: BeautifulSoup) -> str:
        """
        Уникальный учетный номер организации
        """
        try:
            link = soup.find("span", class_="cardMainInfo__content").find("a")["href"]
            number = re.search("Code=[0-9]+", link)
            if number is None:
                number = re.search("Id=[0-9]+", link)
                if number is None:
                    return None, None
                else:
                    return None, re.search("[0-9]+", number[0])[0]
            else:
                return re.search("[0-9]+", number[0])[0], None
        except AttributeError:
            self.logger.info("Не выделено Уникальный учетный номер организации")
            return None

    def find_id_customer(self, soup: BeautifulSoup) -> str:
        """
        идентификационный код заказчика
        """
        try:
            soup = soup.find("span", string="Идентификационный код заказчика")
            code = soup.parent.find("span", class_="section__info")
            code = code.get_text()
            return self.remove_bad_symbols(code)
        except AttributeError:
            return None

    def find_inn_customer(self, soup: BeautifulSoup) -> str:
        """
        ИНН заказчика
        """
        try:
            soup = soup.find("span", string="ИНН")
            code = soup.parent.find("span", class_="section__info")
            code = code.get_text()
            return self.remove_bad_symbols(code)
        except AttributeError:
            self.logger.info("Не выделено ИНН заказчика")
            return None

    def find_kpp_customer(self, soup: BeautifulSoup) -> str:
        """
        КПП ЗАКАЗЧИКА
        """
        try:
            soup = soup.find("span", string="КПП")
            code = soup.parent.find("span", class_="section__info")
            code = code.get_text()
            return self.remove_bad_symbols(code)
        except AttributeError:
            return None

    def find_code_form_org(self, soup: BeautifulSoup) -> str:
        """
        Код организационно-правовой формы
        """
        try:
            soup = soup.find("span", string="Код организационно-правовой формы")
            code = soup.parent.find("span", class_="section__info")
            code = code.get_text()
            return self.remove_bad_symbols(code)
        except AttributeError:
            return None

    def find_okpo_code(self, soup: BeautifulSoup) -> str:
        try:
            soup = soup.find("span", string="Код ОКПО")
            code = soup.parent.find("span", class_="section__info")
            code = code.get_text()
            return self.remove_bad_symbols(code)
        except AttributeError:
            return None

    def find_municipal_code(self, soup: BeautifulSoup) -> str:
        """
        Код территории муниципального образования
        """
        try:
            soup = soup.find("span", string="Код территории муниципального образования")
            code = soup.parent.find("span", class_="section__info")
            code = code.get_text()
            return self.remove_bad_symbols(code)
        except AttributeError:
            return None

    def find_budget_name(self, soup: BeautifulSoup) -> str:
        """
        Наименование бюджета
        """
        try:
            soup_1 = soup.find("span", string="Наименование бюджета")
            name = soup_1.parent.find("span", class_="section__info")
            name = name.get_text()
            return self.remove_bad_symbols(name), None
        except AttributeError:
            try:
                soup = soup.find("span", string="Наименование внебюджетных средств")
                name = soup.parent.find("span", class_="section__info")
                name = name.get_text()
                return None, self.remove_bad_symbols(name)
            except AttributeError:
                self.logger.info("Не выделено Имя бюджетных/внебюджетных средств")
                return None, None

    def find_budget_level(self, soup: BeautifulSoup) -> str:
        """
        Уровень бюджета
        """
        try:
            soup = soup.find("span", string="Уровень бюджета")
            code = soup.parent.find("span", class_="section__info")
            code = code.get_text()
            return self.remove_bad_symbols(code)
        except AttributeError:
            return None

    # ОБЩАЯ ИНФОРМАЦИЯ
    def find_contract_status(self, soup: BeautifulSoup) -> str:
        """
        Статус контракта
        """
        try:
            soup = soup.find("span", string="Статус контракта")
            code = soup.parent.find("span", class_="section__info")
            code = code.get_text()
            return self.remove_bad_symbols(code)
        except AttributeError:
            self.logger.info("Не выделено Статус контракта")
            return None

    def find_notice(self, soup: BeautifulSoup) -> str:
        """
        Номер извещения об осуществлении закупки
        """
        try:
            soup = soup.find("span", string="Номер извещения об осуществлении закупки")
            code = soup.parent.find("span", class_="section__info")
            code = code.get_text()
            return self.remove_bad_symbols(code)
        except AttributeError:
            return None

    def find_ikz_code(self, soup: BeautifulSoup) -> str:
        """
        Идентификационный код закупки (ИКЗ)
        """
        try:
            soup = soup.find("span", string="Идентификационный код закупки (ИКЗ)")
            code = soup.parent.find("span", class_="section__info")
            code = code.get_text()
            return self.remove_bad_symbols(code)
        except AttributeError:
            return None

    def find_id_contract_electronic(self, soup: BeautifulSoup) -> str:
        """
        Идентификатор контракта, заключенного в электронной форме
        """
        try:
            soup = soup.find(
                "span", string="Идентификатор контракта, заключенного в электронной форме"
            )
            code = soup.parent.find("span", class_="section__info")
            code = code.get_text()
            return self.remove_bad_symbols(code)
        except AttributeError:
            return None

    def find_unique_number_plan(self, soup: BeautifulSoup) -> str:
        """
        Уникальный номер позиции плана-графика
        """
        try:
            soup = soup.find("span", string="Уникальный номер позиции плана-графика")
            number = soup.parent.find("span", class_="section__info")
            number = number.get_text()
            return self.remove_bad_symbols(number)
        except AttributeError:
            return None

    def find_method_determinig_supplier(self, soup: BeautifulSoup) -> str:
        """
        Способ определения поставщика (подрядчика, исполнителя)
        """
        try:
            soup = soup.find(
                "span", string="Способ определения поставщика (подрядчика, исполнителя)"
            )
            number = soup.parent.find("span", class_="section__info")
            number = number.get_text()
            return self.remove_bad_symbols(number)
        except AttributeError:
            self.logger.info("Способ определения поставщика (подрядчика, исполнителя)")
            return None

    def find_date_summarizing(self, soup: BeautifulSoup) -> str:
        """
        Дата подведения результатов определения поставщика (подрядчика, исполнителя)
        """
        s = "Дата подведения результатов определения поставщика (подрядчика, исполнителя)"
        try:
            soup = soup.find("span", string=s)
            date = soup.parent.find("span", class_="section__info")
            date = date.get_text()
            return self.remove_bad_symbols(date)
        except AttributeError:
            return None

    def find_date_posting(self, soup: BeautifulSoup) -> str:
        try:
            soup = soup.find("span", string="Дата размещения (по местному времени)")
            date = soup.parent.find("span", class_="section__info")
            date = date.get_text()
            return self.remove_bad_symbols(date)
        except AttributeError:
            return None

    def find_grouds_single_supplier(self, soup: BeautifulSoup) -> str:
        """
        Основание заключения контракта с единственным поставщиком
        """
        try:
            soup = soup.find(
                "span", string="Основание заключения контракта с единственным поставщиком"
            )
            grouds = soup.parent.find("span", class_="section__info")
            grouds = grouds.get_text()
            return self.remove_bad_symbols(grouds)
        except AttributeError:
            return None

    def find_document_details(self, soup: BeautifulSoup) -> str:
        """
        Реквизиты документа, подтверждающего основание заключения контракта
        """
        try:
            soup = soup.find(
                "span",
                string="Реквизиты документа, подтверждающего основание заключения контракта",
            )
            date = soup.parent.find("span", class_="section__info")
            date = date.get_text()
            return self.remove_bad_symbols(date)
        except AttributeError:
            return None

    def find_info_support(self, soup: BeautifulSoup) -> str:
        """
        Информация о банковском и (или) казначейском сопровождении контракта
        """
        s = "Информация о банковском и (или) казначейском сопровождении контракта"
        try:
            soup = soup.find("span", string=s)
            date = soup.parent.find("span", class_="section__info")
            date = date.get_text()
            return self.remove_bad_symbols(date)
        except AttributeError:
            return None

    # ОБЩИЕ ДАННЫЕ
    def find_date_contract(self, soup: BeautifulSoup) -> str:
        """
        Дата заключения контракта
        """
        try:
            soup = soup.find("span", string="Заключение контракта")
            number = soup.parent.find("span", class_="cardMainInfo__content")
            number = number.get_text()
            return self.remove_bad_symbols(number)
        except AttributeError:
            self.logger.info("Не выделено Дата Заключения контракта")
            return None

    def find_date_performance(self, soup: BeautifulSoup) -> str:
        """
        Срок исполнения (дата)
        """
        try:
            soup = soup.find("span", string="Срок исполнения")
            date = soup.parent.find("span", class_="cardMainInfo__content")
            date = date.get_text()
            return self.remove_bad_symbols(date)
        except AttributeError:
            self.logger.info("Не выделено Срок исполнения")
            return None

    def find_date_contract_registry(self, soup: BeautifulSoup) -> str:
        """
        Дата размещения контракт в реестре контрактов
        """
        try:
            soup = soup.find("span", string="Размещен контракт в реестре контрактов")
            date = soup.parent.find("span", class_="cardMainInfo__content")
            date = date.get_text()
            return self.remove_bad_symbols(date)
        except AttributeError:
            return None

    def find_date_update_registry(self, soup: BeautifulSoup) -> str:
        """
        Дата обновления контракт в реестре контрактов
        """
        try:
            soup = soup.find("span", string="Обновлен контракт в реестре контрактов")
            date = soup.parent.find("span", class_="cardMainInfo__content")
            date = date.get_text()
            return self.remove_bad_symbols(date)
        except AttributeError:
            return None

    def find_date_start_performance(self, soup: BeautifulSoup) -> str:
        """
        Дата начала исполнения контракта
        """
        try:
            soup = soup.find("span", string="Дата начала исполнения контракта")
            date = soup.parent.find("span", class_="section__info")
            date = date.get_text()
            return self.remove_bad_symbols(date)
        except AttributeError:
            self.logger.info("Не выделено Дата начала исполнения контракта")
            return None

    def find_date_end_performance(self, soup: BeautifulSoup) -> str:
        """
        Дата окончания исполнения контракта
        """
        try:
            soup = soup.find("span", string="Дата окончания исполнения контракта")
            date = soup.parent.find("span", class_="section__info")
            date = date.get_text()
            return self.remove_bad_symbols(date)
        except AttributeError:
            self.logger.info("Не выделено Дата окончания исполнения контракта")
            return None

    def find_contract_item(self, soup: BeautifulSoup) -> str:
        try:
            soup = soup.find("span", string="Предмет контракта")
            item = soup.parent.find("span", class_="section__info")
            item = item.get_text()
            return self.remove_bad_symbols(item)
        except AttributeError:
            self.logger.info("Не выделено Предмет контракта")
            return None

    def find_contract_price(self, soup: BeautifulSoup) -> str:
        """
        Цена контракта
        """
        try:
            soup_firts = soup.find("span", string="Цена контракта")
            price = soup_firts.parent.find("span", class_="cardMainInfo__content cost")
            price = self.remove_bad_symbols(price.get_text())
            price = "".join([i for i in price if i.isdigit() or i in [",", "."]]).replace(
                ",", "."
            )
            if price != "":
                return price
        except AttributeError:
            pass

        try:
            soup = soup.find("div", class_="price")
            price = soup.parent.find("span", class_="cardMainInfo__content cost")
            price = self.remove_bad_symbols(price.get_text())
            price = "".join([i for i in price if i.isdigit() or i in [",", "."]]).replace(
                ",", "."
            )
            if price != "":
                return price
        except AttributeError:
            self.logger.info("Не выделено Цена контракта")
            return None

    def find_contract_price_nds(self, soup: BeautifulSoup) -> str:
        """
        В том числе НДС
        """
        try:
            soup_firts = soup.find("span", string="В том числе НДС")
            price = soup_firts.parent.find("span", class_="section__info")
            price = self.remove_bad_symbols(price.get_text())
            price = "".join([i for i in price if i.isdigit() or i in [",", "."]]).replace(
                ",", "."
            )
            if price != "":
                return price
        except AttributeError:
            return None

    def find_prepayment_amount(self, soup: BeautifulSoup) -> str:
        """
        Размер аванса
        """
        try:
            soup = soup.find("span", string="Размер аванса")
            prepayment = soup.parent.find("span", class_="section__info")
            prepayment = self.remove_bad_symbols(prepayment.get_text())
            return prepayment
        except AttributeError:
            return None

    def find_performance_security(self, soup: BeautifulSoup) -> str:
        """
        Размер обеспечения исполнения контракта, ₽
        """
        try:
            soup = soup.find("span", string="Размер обеспечения исполнения контракта, ₽")
            security_sum = soup.parent.find("span", class_="section__info")
            security_sum = self.remove_bad_symbols(security_sum.get_text())
            security_sum = "".join(
                [i for i in security_sum if i.isdigit() or i in [",", "."]]
            ).replace(",", ".")
            if security_sum != "":
                return security_sum
        except AttributeError:
            return None

    def find_size_performance_quality(self, soup: BeautifulSoup) -> str:
        """
        Размер обеспечения исполнения обязательств по предоставленной
        гарантии качества товаров, работ, услуг
        """

        s_1 = "Размер обеспечения исполнения обязательств по предоставленной"
        s_2 = "гарантии качества товаров, работ, услуг"
        s = f"{s_1} {s_2}"
        try:
            soup = soup.find(
                "span",
                string=s,
            )
            security_sum = soup.parent.find("span", class_="section__info")
            security_sum = self.remove_bad_symbols(security_sum.get_text())
            security_sum = "".join(
                [i for i in security_sum if i.isdigit() or i in [",", "."]]
            ).replace(",", ".")
            if security_sum != "":
                return security_sum
        except AttributeError:
            return None

    def find_warranty_period(self, soup: BeautifulSoup) -> str:
        """
        Срок, на который предоставляется гарантия
        """
        try:
            soup_new = soup.find("h2", string="Срок, на который предоставляется гарантия")
            if soup_new is None:
                soup_new = soup.find("span", string="Срок, на который предоставляется гарантия")
            period = soup_new.parent.find("span", class_="section__info")
            period = period.get_text()
            return self.remove_bad_symbols(period)
        except AttributeError:
            return None

    def find_place_performance(self, soup: BeautifulSoup) -> str:
        """
        Место выполнения контракта
        """
        s = "Информация о месте поставки товара, выполнения работы или оказания услуги"
        try:
            place = soup.find(
                "h2",
                string=s,
                class_="blockInfo__title_sub",
            ).parent.find("span", class_="section__info")

            return self.remove_bad_symbols(place.get_text())
        except AttributeError:
            return None

    # ИНФОРМАЦИЯ О ПОСТАВЩИКАХ
    def find_full_name_supplier(self, soup: BeautifulSoup) -> str:
        """
        Имя поставщика
        """
        try:
            name = soup.next.get_text()
            return self.remove_bad_symbols(name)
        except AttributeError:
            self.logger.info("Не выделено Имя поставщика")
            return None

    def find_code_okpo_supplier(self, soup: BeautifulSoup) -> str:
        """
        Код по ОКПО
        """
        try:
            soup = soup.find("span", string="Код по ОКПО:")
            code = soup.next.next.next
            code = code.get_text()
            return self.remove_bad_symbols(code)
        except AttributeError:
            return None

    def find_inn_supplier(self, soup: BeautifulSoup) -> str:
        """
        ИНН
        """
        try:
            inn = soup.parent.find("span", string="ИНН:").next.next.next
            inn = inn.get_text()
            return self.remove_bad_symbols(inn)
        except AttributeError:
            self.logger.info("Не выделено ИНН поставщика")
            return None

    def find_kpp_supplier(self, soup: BeautifulSoup) -> str:
        """
        КПП
        """
        try:
            kpp = soup.parent.find("span", string="КПП:").next.next.next
            kpp = kpp.get_text()
            return self.remove_bad_symbols(kpp)
        except AttributeError:
            return None

    def find_date_registration_supplier(self, soup: BeautifulSoup) -> str:
        """
        Дата постановки на учет
        """
        try:
            date = soup.parent.find("span", string="Дата постановки на учет:").next.next.next
            date = date.get_text()
            return self.remove_bad_symbols(date)
        except AttributeError:
            return None

    def find_country_code_supplier(self, soup: BeautifulSoup) -> tuple[str, str]:
        """
        Страна, код
        """
        try:
            country_code = soup.get_text()
            country_code = self.remove_bad_symbols(country_code)

            contry_list = []
            code_list = []
            for i in country_code.split():
                if i.isdigit():
                    code_list.append(i)
                else:
                    contry_list.append(i)
            return " ".join(contry_list), "".join(code_list)
        except AttributeError:
            self.logger.info("Не выделено Страна, код")
            return (None, None)

    def find_adress_supplier(self, soup: BeautifulSoup) -> str:
        """
        Адрес Места нахождения
        """
        try:
            adress = soup.get_text()
            adress = self.remove_bad_symbols(adress)
            return adress
        except AttributeError:
            self.logger.info("Не выделено Адрес Места нахождения")
            return None

    def find_postal_adress_supplier(self, soup: BeautifulSoup) -> str:
        """
        Почтовый адресс
        """
        try:
            adress = soup.get_text()
            adress = self.remove_bad_symbols(adress)
            return adress
        except AttributeError:
            self.logger.info("Не выделено Почтовый адресс")
            return None

    def find_contact_supplier(self, soup: BeautifulSoup) -> str:
        """
        Телефон, электронная почта
        """
        try:
            contact = soup.get_text()
            contact = self.remove_bad_symbols(contact)
            return contact
        except AttributeError:
            self.logger.info("Не выделено Телефон, электронная почта")
            return None

    def find_status_supplier(self, soup: BeautifulSoup) -> str:
        """
        Статус
        """
        try:
            status = soup.get_text()
            status = self.remove_bad_symbols(status)

            return status
        except AttributeError:
            self.logger.info("Не выделено Статус")
            return None

    def read_table_supplier(self, soup: BeautifulSoup) -> tuple[list, list]:
        """
        Метод считывает данные о поставщиках
        и возращает два list c объектами BeautifulSoup.
        Один лист с именем колонки, второй с содержанием
        """

        table_names = soup.find(
            "h2", class_="blockInfo__title", string="Информация о поставщиках"
        ).parent.find("tr", class_="tableBlock__row")

        table_values = soup.find(
            "h2", class_="blockInfo__title", string="Информация о поставщиках"
        ).parent.find_all("td", class_="tableBlock__col")

        if table_names is None or table_values is None:
            self.logger.info("Нет данных по поставщику")
            return None, None

        list_table_values = [
            element for element in table_values if type(element) is bs4.element.Tag
        ]
        string = self.remove_bad_symbols(table_names.get_text())
        names_columns = []
        i_start = 0
        i_status = False
        for i in range(len(string)):
            if string[i].isupper() and not i_status:
                i_status = True
                i_start = i
            elif string[i].isupper() and i_status:
                names_columns.append(string[i_start:i])
                i_start = i
            elif i == len(string) - 1:
                i += 1
                names_columns.append(string[i_start:i])
        return names_columns, list_table_values

    # Платежи и объекты закупки
    def find_kbk(self, soup: BeautifulSoup) -> str:
        """
        КБК
        """
        try:
            code = soup.find("td", class_="tableBlock__col")
            code = self.remove_bad_symbols(code.get_text())

            return code.replace(" ", "")
        except AttributeError:
            self.logger.info("Не выделено КБК")
            return None

    def payment_info(self, number_contract: str):
        soup = self.get_page(f"{self.url_payment}{number_contract}")
        if soup is None:
            self.add_to_csv_problem("Контракт payment_info не найден")
            self.logger.info(f"{self.number_contract} Неудача payment_info")
        else:
            self.kbk = self.find_kbk(soup)

    def parsing_supplier(self, soup: BeautifulSoup) -> None:
        """
        Парсинг данных поставщиков
        """
        self.full_name_supplier = None
        self.inn_supplier = None
        self.kpp_supplier = None
        self.code_okpo_supplier = None
        self.date_registration_supplier = None
        self.country_supplier = None
        self.code_country_supplier = None
        self.postal_adress_supplier = None
        self.contact = None
        self.status_supplier = None

        names_columns, list_table_values = self.read_table_supplier(soup)

        if names_columns is None or list_table_values is None:
            self.logger.info("Нет данных о поставщике")
            return None
        for i_step, name_columns in enumerate(names_columns):
            if "Организация" in name_columns:
                self.full_name_supplier = self.find_full_name_supplier(list_table_values[i_step])
                self.inn_supplier = self.find_inn_supplier(list_table_values[i_step])
                self.kpp_supplier = self.find_kpp_supplier(list_table_values[i_step])
                self.code_okpo_supplier = self.find_code_okpo_supplier(list_table_values[i_step])
                self.date_registration_supplier = self.find_date_registration_supplier(
                    list_table_values[i_step]
                )
                continue

            if "Страна, код" in name_columns:
                (
                    self.country_supplier,
                    self.code_country_supplier,
                ) = self.find_country_code_supplier(list_table_values[i_step])
                continue

            if "Адрес места нахождения" in name_columns:
                self.adress_supplier = self.find_adress_supplier(list_table_values[i_step])
                continue

            if "Почтовый адрес" in name_columns:
                self.postal_adress_supplier = self.find_postal_adress_supplier(
                    list_table_values[i_step]
                )
                continue

            if "Телефон, электронная почта" in name_columns:
                self.contact = self.find_contact_supplier(list_table_values[i_step])
                continue

            if "Статус" in name_columns:
                self.status_supplier = self.find_status_supplier(list_table_values[i_step])
                continue

    def add_data_to_csv(self):
        self.dict_columns_table = {
            "number_contract": self.number_contract,
            "adress_customer": self.adress_customer,
            "full_name_customer": self.full_name_customer,
            "short_name_customer": self.short_name_customer,
            "unique_site_code": self.unique_site_code,
            "unique_site_id": self.unique_site_id,
            "id_customer": self.id_customer,
            "inn_customer": self.inn_customer,
            "kpp_customer": self.kpp_customer,
            "code_form_org": self.code_form_org,
            "okpo_code": self.okpo_code,
            "municipal_code": self.municipal_code,
            "budget_name": self.budget_name,
            "extrabudget_name": self.extrabudget_name,
            "budget_level": self.budget_level,
            "contract_status": self.contract_status,
            "notice": self.notice,
            "ikz_code": self.ikz_code,
            "id_contract_electronic": self.id_contract_electronic,
            "unique_number_plan": self.unique_number_plan,
            "method_determinig_supplier": self.method_determinig_supplier,
            "date_summarizing": self.date_summarizing,
            "date_posting": self.date_posting,
            "grouds_single_supplier": self.grouds_single_supplier,
            "document_details": self.document_details,
            "info_support": self.info_support,
            "find_date_contract": self.date_contract,
            "date_performance": self.date_performance,
            "date_contract_registry": self.date_contract_registry,
            "date_update_registry": self.date_update_registry,
            "date_start_performance": self.date_start_performance,
            "date_end_performance": self.date_end_performance,
            "contract_item": self.contract_item,
            "contract_price": self.contract_price,
            "contract_price_nds": self.contract_price_nds,
            "prepayment_amount": self.prepayment_amount,
            "performance_security": self.performance_security,
            "size_performance_quality": self.size_performance_quality,
            "warranty_period": self.warranty_period,
            "place_performance": self.place_performance,
            "full_name_supplier": self.full_name_supplier,
            "inn_supplier": self.inn_supplier,
            "kpp_supplier": self.kpp_supplier,
            "code_okpo_supplier": self.code_okpo_supplier,
            "date_registration_supplier": self.date_registration_supplier,
            "country_supplier": self.country_supplier,
            "code_country_supplier": self.code_country_supplier,
            "adress_supplier": self.adress_supplier,
            "postal_adress_supplier": self.postal_adress_supplier,
            "contact": self.contact,
            "status_supplier": self.status_supplier,
            "kbk": self.kbk,
        }

        pd.DataFrame(
            self.dict_columns_table,
            index=[0],
        ).to_csv(self.path_output, mode="a", index=False, header=False, sep="|")

    def add_to_csv_problem(self, commentary: str):
        self.dict_columns_table_problem = {
            "number_contract": self.number_contract,
            "adress_customer": self.adress_customer,
            "inn_customer": self.inn_customer,
            "commentary": commentary,
        }
        pd.DataFrame(
            self.dict_columns_table_problem,
            index=[0],
        ).to_csv(self.path_contract_problem, mode="a", index=False, header=False, sep="|")

    def run(self):
        self.initialize()
        for index in tqdm(range(len(self.df_input))):
            number_contract = self.df_input.loc[index, "number_contract"]
            self.logger.info(f"number_contract {number_contract}")
            self.adress_customer = self.df_input.loc[index, "adress_customer"]
            soup = self.get_page(f"{self.url_info}{number_contract}")
            if soup is None:
                self.number_contract = number_contract
                self.inn_customer = self.df_input.loc[index, "inn_customer"]
                self.add_to_csv_problem("Контракт info не найден")
                self.logger.info(f"{self.number_contract} Неудача info")
                continue
            # Информация о заказчике
            self.number_contract = number_contract
            self.full_name_customer = self.find_full_name_customer(soup)
            self.short_name_customer = self.find_short_name_customer(soup)
            self.unique_site_code, self.unique_site_id = self.find_unique_account_number(soup)
            self.id_customer = self.find_id_customer(soup)
            self.inn_customer = self.find_inn_customer(soup)
            self.kpp_customer = self.find_kpp_customer(soup)
            self.code_form_org = self.find_code_form_org(soup)
            self.okpo_code = self.find_okpo_code(soup)
            self.municipal_code = self.find_municipal_code(soup)
            self.budget_name, self.extrabudget_name = self.find_budget_name(soup)
            self.budget_level = self.find_budget_level(soup)
            self.contract_status = self.find_contract_status(soup)
            self.notice = self.find_notice(soup)
            self.ikz_code = self.find_ikz_code(soup)
            self.id_contract_electronic = self.find_id_contract_electronic(soup)
            self.unique_number_plan = self.find_unique_number_plan(soup)
            self.method_determinig_supplier = self.find_method_determinig_supplier(soup)
            self.date_summarizing = self.find_date_summarizing(soup)
            self.date_posting = self.find_date_posting(soup)
            self.grouds_single_supplier = self.find_grouds_single_supplier(soup)
            self.document_details = self.find_document_details(soup)
            self.info_support = self.find_info_support(soup)

            # Общие данные
            self.date_contract = self.find_date_contract(soup)
            self.date_performance = self.find_date_performance(soup)
            self.date_contract_registry = self.find_date_contract_registry(soup)
            self.date_update_registry = self.find_date_update_registry(soup)
            self.date_start_performance = self.find_date_start_performance(soup)
            self.date_end_performance = self.find_date_end_performance(soup)
            self.contract_item = self.find_contract_item(soup)
            self.contract_price = self.find_contract_price(soup)
            self.contract_price_nds = self.find_contract_price_nds(soup)
            self.prepayment_amount = self.find_prepayment_amount(soup)
            self.performance_security = self.find_performance_security(soup)
            self.size_performance_quality = self.find_size_performance_quality(soup)
            self.warranty_period = self.find_warranty_period(soup)
            self.place_performance = self.find_place_performance(soup)

            # Информация о поставщиках
            self.parsing_supplier(soup)

            # Платежи и объекты закупки
            self.payment_info(number_contract)

            self.add_data_to_csv()
        self.logger_print.info(f"Успешно завершено {self.file_name}")


def test():
    get_num = ParsingDataContract(
        path_df="data/contract_number/split_data_test/2014/1.csv",
        path_output="data/test_raw_data",
        path_dir_log="logs/parsing_data",
        path_contract_problem="data/contract_number/problem_contract",
        continue_parsing=False,
    )
    get_num.run()


if __name__ == "__main__":
    test()