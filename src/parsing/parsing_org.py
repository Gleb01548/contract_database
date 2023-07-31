import os
import logging
import re
import time

from tqdm import tqdm
import requests
import pandas as pd
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
    ):
        """
        Метод парсит данные о заказчиках с сайта https://zakupki.gov.ru/
        """

        self.path_df = path_df
        self.path_output = path_output
        self.path_dir_log = path_dir_log
        self.continue_parsing = continue_parsing
        self.path_contract_problem = path_contract_problem

        self.url_org = "https://zakupki.gov.ru/epz/organization/view/info.html?organization"

        self.list_columns_table = [
            "code",
            "code_type",
            "full_name",
            "short_name",
            "code_registr",
            "date_registration",
            "date_last_change",
            "inn",
            "kpp",
            "ogrn",
            "oktmo",
            "location",
            "iky",
            "date_iky",
            "code_okfs",
            "name_property",
            "okpf_code",
            "okopf_name",
            "credentials",
            "date_registration_tax",
            "organization_type",
            "organization_level",
            "okpo_code",
            "okfd_code",
            "budget_code",
            "budget_name",
            "telephone",
            "fax",
            "postal_adress",
            "email",
            "site",
            "contact_person",
            "time_zone",
        ]

    def initialize(self):
        dir_name = os.path.basename(os.path.dirname(self.path_df))

        self.path_output = os.path.join(
            self.path_output, dir_name, os.path.basename(self.path_df)
        )

        # если нет, то создаем папку куда сложим результаты работы программы
        if not os.path.exists(os.path.dirname(self.path_output)):
            os.makedirs(os.path.dirname(self.path_output))

        # если нет, то создаем файл в который будем записывать результат
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

    def make_links(self, code: str, code_type: str) -> str:
        url_org_code = self.url_org + code_type + "=" + code
        url_org_another_code = url_org_code + "&tab=other"
        return url_org_code, url_org_another_code

    def make_continue_parsing(self):
        df_output = pd.read_csv(self.path_output, sep="|", dtype="str")
        print()
        last_code = df_output["code"].iloc[-1]

        last_index = self.df_input["code"].to_list().index(last_code)
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
        list_code_check = [
            "717774",
            "674905",
            "711266",
            "745661",
            "674905",
            "802777",
        ]
        for code in list_code_check:
            url = self.url_org + "Id" + "=" + code
            res = requests.get(url, headers={"User-Agent": UserAgent().random})
            if not res.ok:
                continue
            else:
                return None
        warn = "НА САЙТЕ ТЕХНИЧЕСКИЕ РАБОТЫ"
        self.logger.warning(warn)
        raise IOError(warn)

    def find_full_name(self, soup: BeautifulSoup) -> str:
        """
        Полное имя организации
        """
        try:
            name = soup.find("span", class_="section__title", string="Полное наименование")
            name = name.parent.find("span", class_="section__info")
            name = self.remove_bad_symbols(name.get_text())
            return name
        except AttributeError:
            warn = "НЕТ ПОЛНОГО ИМЕНИ ОРГАНИЗАЦИИ"
            self.logger.warning(warn)
            return None

    def find_short_name(self, soup: BeautifulSoup) -> str:
        """
        Сокращенное имя организации
        """
        try:
            name = soup.find("span", class_="section__title", string="Сокращенное наименование")
            name = name.parent.find("span", class_="section__info")
            name = self.remove_bad_symbols(name.get_text())
            return name
        except AttributeError:
            return None

    def find_code_registr(self, soup: BeautifulSoup) -> str:
        """
        Код по сводному регистру
        """
        try:
            code = soup.find("span", class_="section__title", string="Код по Сводному реестру")
            code = code.parent.find("span", class_="section__info")
            code = self.remove_bad_symbols(code.get_text())
            return code
        except AttributeError:
            return None

    def find_date_registration(self, soup: BeautifulSoup) -> str:
        """
        Дата регистрации
        """
        try:
            code = soup.find("span", class_="section__title", string="Дата регистрации")
            code = code.parent.find("span", class_="section__info")
            code = self.remove_bad_symbols(code.get_text())
            return code
        except AttributeError:
            return None

    def find_date_last_change(self, soup: BeautifulSoup) -> str:
        """
        Дата/время последнего изменения записи об организации
        """
        try:
            code = soup.find(
                "span",
                class_="section__title",
                string="Дата/время последнего изменения сведений об организации",
            )
            code = code.parent.find("span", class_="section__info")
            code = self.remove_bad_symbols(code.get_text())
            return code
        except AttributeError:
            return None

    def find_inn(self, soup: BeautifulSoup) -> str:
        """
        ИНН
        """
        try:
            code = soup.find(
                "span",
                class_="section__title",
                string="ИНН",
            )
            code = code.parent.find("span", class_="section__info")
            code = self.remove_bad_symbols(code.get_text())
            return code
        except AttributeError:
            warn = "ИНН не найден"
            self.logger.warning(warn)
            return None

    def find_kpp(self, soup: BeautifulSoup) -> str:
        """
        КПП
        """
        try:
            code = soup.find(
                "span",
                class_="section__title",
                string="КПП",
            )
            code = code.parent.find("span", class_="section__info")
            code = self.remove_bad_symbols(code.get_text())
            return code
        except AttributeError:
            return None

    def find_ogrn(self, soup: BeautifulSoup) -> str:
        """
        ОГРН
        """
        try:
            code = soup.find(
                "span",
                class_="section__title",
                string="ОГРН",
            )
            code = code.parent.find("span", class_="section__info")
            code = self.remove_bad_symbols(code.get_text())
            return code
        except AttributeError:
            return None

    def find_oktmo(self, soup: BeautifulSoup) -> str:
        """
        ОКТМО
        """
        try:
            code = soup.find(
                "span",
                class_="section__title",
                string="ОКТМО",
            )
            code = code.parent.find("span", class_="section__info")
            code = self.remove_bad_symbols(code.get_text())
            return code
        except AttributeError:
            return None

    def find_location(self, soup: BeautifulSoup) -> str:
        """
        Место нахождения
        """
        try:
            code = soup.find(
                "span",
                class_="section__title",
                string="Место нахождения",
            )
            code = code.parent.find("span", class_="section__info")
            code = self.remove_bad_symbols(code.get_text())
            return code
        except AttributeError:
            return None

    def find_iky(self, soup: BeautifulSoup) -> str:
        """
        ИКУ
        """
        try:
            code = soup.find(
                "span",
                class_="section__title",
                string="ИКУ",
            )
            code = code.parent.find("span", class_="section__info")
            code = self.remove_bad_symbols(code.get_text())
            return code
        except AttributeError:
            return None

    def find_date_iky(self, soup: BeautifulSoup) -> str:
        """
        Дата присвоения ИКУ
        """
        try:
            code = soup.find(
                "span",
                class_="section__title",
                string="Дата присвоения ИКУ",
            )
            code = code.parent.find("span", class_="section__info")
            code = self.remove_bad_symbols(code.get_text())
            return code
        except AttributeError:
            return None

    def find_code_okfs(self, soup: BeautifulSoup) -> str:
        """
        Код по ОКФС
        """
        try:
            code = soup.find(
                "span",
                class_="section__title",
                string="Код по ОКФС",
            )
            code = code.parent.find("span", class_="section__info")
            code = self.remove_bad_symbols(code.get_text())
            return code
        except AttributeError:
            return None

    def find_name_property(self, soup: BeautifulSoup) -> str:
        """
        Наименование типа собственности
        """
        try:
            code = soup.find(
                "span",
                class_="section__title",
                string="Наименование",
            )
            code = code.parent.find("span", class_="section__info")
            code = self.remove_bad_symbols(code.get_text())
            return code
        except AttributeError:
            return None

    def find_code_okpf(self, soup: BeautifulSoup) -> str:
        """
        Код по ОКОПФ
        """
        try:
            code = soup.find(
                "span",
                class_="section__title",
                string="Код по ОКОПФ",
            )
            code = code.parent.find("span", class_="section__info")
            code = self.remove_bad_symbols(code.get_text())
            return code
        except AttributeError:
            return None

    def find_okopf_name(self, soup: BeautifulSoup) -> str:
        """
        Имя по ОКОПФ
        """
        try:
            code = soup.find_all(
                "span",
                class_="section__title",
                string="Наименование",
            )[1]
            code = code.parent.find("span", class_="section__info")
            code = self.remove_bad_symbols(code.get_text())
            return code
        except (AttributeError, IndexError):
            return None

    def find_credentials(self, soup: BeautifulSoup) -> str:
        """
        Полномочия организации
        """
        try:
            code = soup.find(
                "span",
                class_="section__title",
                string="Полномочия организации",
            )
            code = code.parent.find("span", class_="section__info")
            code = self.remove_bad_symbols(code.get_text())
            return code
        except AttributeError:
            return None

    def find_date_registration_tax(self, soup: BeautifulSoup) -> str:
        """
        Дата постановки организации на учет в налоговом органе
        """
        try:
            code = soup.find(
                "span",
                class_="section__title",
                string="Дата постановки организации на учет в налоговом органе",
            )
            code = code.parent.find("span", class_="section__info")
            code = self.remove_bad_symbols(code.get_text())
            return code
        except AttributeError:
            return None

    def find_organization_type(self, soup: BeautifulSoup) -> str:
        """
        Тип организации
        """
        try:
            code = soup.find(
                "span",
                class_="section__title",
                string="Тип организации",
            )
            code = code.parent.find("span", class_="section__info")
            code = self.remove_bad_symbols(code.get_text())
            return code
        except AttributeError:
            return None

    def find_organization_level(self, soup: BeautifulSoup) -> str:
        """
        Уровень организации
        """
        try:
            code = soup.find(
                "span",
                class_="section__title",
                string="Уровень организации",
            )
            code = code.parent.find("span", class_="section__info")
            code = self.remove_bad_symbols(code.get_text())
            return code
        except AttributeError:
            return None

    def find_okpo_code(self, soup: BeautifulSoup) -> str:
        """
        ОКПО
        """
        try:
            code = soup.find(
                "span",
                class_="section__title",
                string="ОКПО",
            )
            code = code.parent.find("span", class_="section__info")
            code = self.remove_bad_symbols(code.get_text())
            return code
        except AttributeError:
            return None

    def find_okfd_code(self, soup: BeautifulSoup) -> str:
        """
        ОКВЭД
        """
        try:
            code = soup.find(
                "span",
                class_="section__title",
                string="ОКВЭД",
            )
            code = code.parent.find("span", class_="section__info")
            code = self.remove_bad_symbols(code.get_text())
            return code
        except AttributeError:
            return None

    def find_budget_table(self, soup: BeautifulSoup) -> str:
        """
        Таблица бюджета
        """
        try:
            table = soup.find(
                "h2",
                class_="blockInfo__title",
                string="Бюджеты",
            )
            table = table.parent.find("tr", class_="tableBlock__row")
            return self.remove_bad_symbols(table.get_text())
        except AttributeError:
            return None

    def find_budget_data(self, soup: BeautifulSoup) -> str:
        """
        Данные по бюджету
        """
        try:
            data = soup.find(
                "h2",
                class_="blockInfo__title",
                string="Бюджеты",
            )
            data = data.parent.find_all("tr", class_="tableBlock__row")[1].find_all("td")
            return data
        except AttributeError:
            return None

    def parsing_budget_table(self, soup):
        """
        Извлечение данных из таблицы
        """
        string = self.find_budget_table(soup)
        if string is None:
            return None, None
        stri = []
        i_start = 0
        i_status = False
        for i in range(len(string)):
            if string[i].isupper() and not i_status:
                i_status = True
                i_start = i
            elif string[i].isupper() and i_status:
                i_1 = i - 1
                stri.append(string[i_start:i_1])
                i_start = i
            elif i == len(string) - 1:
                i_2 = i + 1
                stri.append(string[i_start:i_2])

        budget_data = self.find_budget_data(soup)
        if budget_data is None:
            return None, None

        code_budget = None
        name_budget = None
        for i_step, i in enumerate(stri):
            i_step += 1
            if "Код" in i:
                code_budget = self.remove_bad_symbols(budget_data[i_step].get_text())
            if "Наименование" in i:
                name_budget = self.remove_bad_symbols(budget_data[i_step].get_text())
        return code_budget, name_budget

    def find_contact_info(self, soup: BeautifulSoup) -> str:
        """
        Контактная информация
        """
        try:
            info = soup.find(
                "h2", class_="blockInfo__title", string="Контактная информация"
            ).parent
            return info
        except AttributeError:
            return None

    def find_telephone(self, soup: BeautifulSoup) -> str:
        """
        Телефон
        """
        try:
            soup = soup.find("span", class_="section__title", string="Телефон")
            telephone = soup.parent.find("span", class_="section__info")
            return self.remove_bad_symbols(telephone.get_text())
        except AttributeError:
            return None

    def find_fax(self, soup: BeautifulSoup) -> str:
        """
        Факс
        """
        try:
            soup = soup.find("span", class_="section__title", string="Факс")
            telephone = soup.parent.find("span", class_="section__info")
            return self.remove_bad_symbols(telephone.get_text())
        except AttributeError:
            return None

    def find_postal_adress(self, soup: BeautifulSoup) -> str:
        """
        Почтовый адрес
        """
        try:
            soup = soup.find("span", class_="section__title", string="Почтовый адрес")
            telephone = soup.parent.find("span", class_="section__info")
            return self.remove_bad_symbols(telephone.get_text())
        except AttributeError:
            return None

    def find_email(self, soup: BeautifulSoup) -> str:
        """
        Электронная почта
        """
        try:
            soup = soup.find(
                "span", class_="section__title", string="Контактный адрес электронной почты"
            )
            telephone = soup.parent.find("span", class_="section__info")
            return self.remove_bad_symbols(telephone.get_text())
        except AttributeError:
            return None

    def find_site(self, soup: BeautifulSoup) -> str:
        """
        Сайт
        """
        try:
            soup = soup.find(
                "span", class_="section__title", string="Адрес организации в сети Интернет"
            )
            telephone = soup.parent.find("span", class_="section__info")
            return self.remove_bad_symbols(telephone.get_text())
        except AttributeError:
            return None

    def find_contact_person(self, soup: BeautifulSoup) -> str:
        """
        Контактное лицо
        """
        try:
            soup = soup.find("span", class_="section__title", string="Контактное лицо")
            telephone = soup.parent.find("span", class_="section__info")
            return self.remove_bad_symbols(telephone.get_text())
        except AttributeError:
            return None

    def find_time_zone(self, soup: BeautifulSoup) -> str:
        """
        Часовая зона
        """
        try:
            soup = soup.find("span", class_="section__title", string="Часовая зона")
            telephone = soup.parent.find("span", class_="section__info")
            return self.remove_bad_symbols(telephone.get_text())
        except AttributeError:
            return None

    def add_data_to_csv(self):
        self.dict_columns_table = {
            "code": self.code,
            "code_type": self.code_type,
            "full_name": self.full_name,
            "short_name": self.short_name,
            "code_registr": self.code_registr,
            "date_registration": self.date_registration,
            "date_last_change": self.date_last_change,
            "inn": self.inn,
            "kpp": self.kpp,
            "ogrn": self.ogrn,
            "oktmo": self.oktmo,
            "location": self.location,
            "iky": self.iky,
            "date_iky": self.date_iky,
            "code_okfs": self.code_okfs,
            "name_property": self.name_property,
            "okpf_code": self.okpf_code,
            "okopf_name": self.okopf_name,
            "credentials": self.credentials,
            "date_registration_tax": self.date_registration_tax,
            "organization_type": self.organization_type,
            "organization_level": self.organization_level,
            "okpo_code": self.okpo_code,
            "okfd_code": self.okfd_code,
            "budget_code": self.budget_code,
            "budget_name": self.budget_name,
            "telephone": self.telephone,
            "fax": self.fax,
            "postal_adress": self.postal_adress,
            "email": self.email,
            "site": self.site,
            "contact_person": self.contact_person,
            "time_zone": self.time_zone,
        }

        pd.DataFrame(
            self.dict_columns_table,
            index=[0],
        ).to_csv(self.path_output, mode="a", index=False, header=False, sep="|")

    def add_to_csv_problem(self, commentary: str):
        self.dict_columns_table_problem = {
            "type": self.code_type,
            "code": self.code,
            "commentary": commentary,
        }
        pd.DataFrame(
            self.dict_columns_table_problem,
            index=[0],
        ).to_csv(self.path_contract_problem, mode="a", index=False, header=False, sep="|")

    def run(self):
        self.initialize()
        for index in tqdm(range(len(self.df_input))):
            self.code = self.df_input.loc[index, "code"]
            self.code_type = self.df_input.loc[index, "code_type"]
            link, link_another = self.make_links(self.code, self.code_type)

            self.logger.info(f"code {self.code}")
            soup = self.get_page(link)
            if soup is None:
                self.add_to_csv_problem("Контракт info не найден")
                self.logger.info(f"{self.code} {self.code_type} Неудача info")
                continue
            # Информация о заказчике
            self.full_name = self.find_full_name(soup)
            self.short_name = self.find_short_name(soup)
            self.code_registr = self.find_code_registr(soup)
            self.date_registration = self.find_date_registration(soup)
            self.date_last_change = self.find_date_last_change(soup)
            self.inn = self.find_inn(soup)
            self.kpp = self.find_kpp(soup)
            self.ogrn = self.find_ogrn(soup)
            self.oktmo = self.find_oktmo(soup)
            self.location = self.find_location(soup)
            self.iky = self.find_iky(soup)
            self.date_iky = self.find_iky(soup)
            self.code_okfs = self.find_code_okfs(soup)
            self.name_property = self.find_name_property(soup)
            self.okpf_code = self.find_okfd_code(soup)
            self.okopf_name = self.find_okopf_name(soup)

            # парсин вкладки дополнительная информация
            soup = self.get_page(link_another)
            if soup is None:
                self.add_to_csv_problem("Полномочия another не найдены")
                self.logger.info(f"{self.code} {self.code_type} Неудача another")
                continue

            self.credentials = self.find_credentials(soup)
            self.date_registration_tax = self.find_date_registration_tax(soup)
            self.organization_type = self.find_organization_type(soup)
            self.organization_level = self.find_organization_level(soup)
            self.okpo_code = self.find_okpo_code(soup)
            self.okfd_code = self.find_okfd_code(soup)
            self.budget_code, self.budget_name = self.parsing_budget_table(soup)
            self.telephone = self.find_telephone(soup)
            self.fax = self.find_fax(soup)
            self.postal_adress = self.find_postal_adress(soup)
            self.email = self.find_email(soup)
            self.site = self.find_site(soup)
            self.contact_person = self.find_contact_person(soup)
            self.time_zone = self.find_time_zone(soup)
            self.add_data_to_csv()

        self.logger_print.info(f"Успешно завершено {self.file_name}")


def test():
    get_num = ParsingOrg(
        path_df="data/contract_number/code_id/2014/code.csv",
        path_output="data/raw_data/org",
        path_dir_log="logs/parsing_org",
        path_contract_problem="data/contract_number/problem_contract/parsing_org",
        continue_parsing=True,
    )
    get_num.run()


if __name__ == "__main__":
    test()
