import os
import logging
import datetime
import re
import time

import requests
import pandas as pd
import bs4.element
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

    def get_page(url: str) -> BeautifulSoup:
        """
        Метод считывает заданную страницу,
        а потом преобразует в lxml
        """
        for _ in range(10):
            res = requests.get(url, headers={"User-Agent": UserAgent().random})
            if res is not None and res.ok:
                return BeautifulSoup(res.text, "lxml")

    def remove_bad_symbols(string: str) -> str:
        string = re.sub("\n|\||\xa0", "", string.strip())
        return " ".join(string.split())

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
            self.logger.info("Не выделено Идентификационный код заказчика")
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
            self.logger.info("Не выделено КПП заказчика")
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
            self.logger.info("Не выделено Код организационно-правовой формы")
            return None

    def find_okpo_code(self, soup: BeautifulSoup) -> str:
        try:
            soup = soup.find("span", string="Код ОКПО")
            code = soup.parent.find("span", class_="section__info")
            code = code.get_text()
            return self.remove_bad_symbols(code)
        except AttributeError:
            self.logger.info("Не выделено Код ОКПО")
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
            self.logger.info("Не выделено Код территории муниципального образования")
            return None

    def find_budget_name(self, soup: BeautifulSoup) -> str:
        """
        Наименование бюджета
        """
        try:
            soup = soup.find("span", string="Наименование бюджета")
            code = soup.parent.find("span", class_="section__info")
            code = code.get_text()
            return self.remove_bad_symbols(code)
        except AttributeError:
            self.logger.info("Не выделено Наименование бюджета")
            return None

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
            self.logger.info("Не выделено Уровень бюджета")
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
            self.logger.info("Не выделено Номер извещения об осуществлении закупки")
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
            self.logger.info("Не выделено Идентификационный код закупки (ИКЗ)")
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
            self.logger.info(
                "Не выделено Идентификатор контракта, заключенного в электронной форме"
            )
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
            self.logger.info("Не выделено Уникальный номер позиции плана-графика")
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
            self.logger.info(f"Не выделено {s}")
            return None

    def find_date_posting(self, soup: BeautifulSoup) -> str:
        try:
            soup = soup.find("span", string="Дата размещения (по местному времени)")
            date = soup.parent.find("span", class_="section__info")
            date = date.get_text()
            return self.remove_bad_symbols(date)
        except AttributeError:
            self.logger.info("Не выделено Дата размещения (по местному времени)")
            return None

    def find_grouds_single_supplier(self, soup: BeautifulSoup) -> str:
        """
        Основание заключения контракта с единственным поставщиком
        """
        try:
            soup = soup.find(
                "span", string="Основание заключения контракта с единственным поставщиком"
            )
            print()
            grouds = soup.parent.find("span", class_="section__info")
            grouds = grouds.get_text()
            return self.remove_bad_symbols(grouds)
        except AttributeError:
            self.logger.info(
                "Не выделено Основание заключения контракта с единственным поставщиком"
            )
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
            self.logger.info(
                "Не выделено Реквизиты документа, подтверждающего основание заключения контракта"
            )
            return None

    def info_support(self, soup: BeautifulSoup) -> str:
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
            self.logger.info(f"Не выделено {s}")
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
            self.logger.info("Не выделено Заключение контракта")
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
            self.logger.info("Не выделено Размещен контракт в реестре контрактов")
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
            self.logger.info("Не выделено Обновлен контракт в реестре контрактов")
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
            self.logger.info("Не выделено В том числе НДС")
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
            self.logger.info("Не выделено Размер аванса")
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
            self.logger.info("Не выделено Размер обеспечения исполнения контракта, ₽")
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
            self.logger.info(f"Не выделено {s}")
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
            self.logger.info("Не выделено Срок, на который предоставляется гарантия")
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
            self.logger.info("Не выделено Код по ОКПО")
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
            self.logger.info("Не выделено ИНН")
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
            self.logger.info("Не выделено КПП")
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
            self.logger.info("Не выделено Дата постановки на учет")
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
            "логги"
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


def parsing_supplier(self, soup: BeautifulSoup) -> None:
    """
    Парсинг данных поставщиков
    """
    names_columns, list_table_values = self.read_table_supplier(soup)

    if names_columns is None or list_table_values is None:
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
            self.country, self.code = self.find_country_code_supplier(list_table_values[i_step])
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
            contact = self.find_contact_supplier(list_table_values[i_step])
            print("contact", contact)
            continue

        if "Статус" in name_columns:
            self.status_supplier = self.find_status_supplier(list_table_values[i_step])
            continue


def run(self):
    self.initialize()
    self.init_logger()

    for index in range(len(self.path_df)):
        number_contract = self.path_df.loc["number_contract", index]
        self.adress_customer = self.path_df.loc["adress_customer", index]
        soup = self.get_page(f"{self.url_info}{number_contract}")

        # Информация о заказчике
        self.full_name_customer = self.find_full_name_customer(soup)
        self.short_name_customer = self.find_short_name_customer(soup)
        self.id_customer = self.find_id_customer(soup)
        self.inn_customer = self.find_inn_customer(soup)
        self.kpp_customer = self.find_kpp_customer(soup)
        self.code_form_org = self.find_code_form_org(soup)
        self.okpo_code = self.find_okpo_code(soup)
        self.municipal_code = self.find_municipal_code(soup)
        self.budget_name = self.find_budget_name(soup)
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
        self.info_support = self.info_support(soup)

        # Общие данные
        self.find_date_contract = self.find_date_contract(soup)
        self.date_performance = self.find_date_performance(soup)
