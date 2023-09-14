import re
import os
import datetime

import pandas as pd
from pandas import DataFrame, Series

from src import DecompositionAddress
from src.constants import (
    dict_month,
    list_local,
    list_sub,
    list_fed,
    list_fed_2,
    list_local_2,
    list_sub_2,
    list_anothe,
    list_fed_3,
    list_sub_3,
    list_local_3,
    inn_sub,
    inn_fed,
    inn_mun,
    inn_another,
    # name_for_result,
)


class ProcessingData:
    def __init__(
        self,
        path_cache_address: str,
        path_cache_org_address: str,
        path_kbk_table: str,
        default_year_for_kbk: str,
    ):
        self.address_dec = DecompositionAddress(
            path_for_cache=path_cache_address, year=default_year_for_kbk
        )

        self.path_cache_org_address = path_cache_org_address
        if not os.path.exists(path_cache_org_address):
            pd.DataFrame(columns=["code", "code_type", "address", "year"]).to_csv(
                path_cache_org_address, sep="|", index=False
            )

        # подготовка кэша адрессов
        df_cahce = pd.read_csv(path_cache_org_address, sep="|", dtype="str")
        self.columns_cache = list(df_cahce.columns)
        df_cahce["unique"] = df_cahce["code"] + "_" + df_cahce["code_type"]
        df_cahce = df_cahce[["unique", "address"]].set_index("unique")

        self.cache_org_address = df_cahce.to_dict(orient="index")
        self.kbk_type = pd.read_excel(path_kbk_table, sheet_name="type", dtype="str")
        self.kbk_np = pd.read_excel(path_kbk_table, sheet_name="np", dtype="str")
        self.kbk_section = pd.read_excel(path_kbk_table, sheet_name="section", dtype="str")

        self.default_year_for_kbk = default_year_for_kbk

        self.address_level = [
            "country",
            "regioncity",
            "regionarea",
            "district",
            "settlement",
            "city",
            "citydistrict",
            "locality",
            "territory",
            "street",
            "plot",
            "building",
            "apartment",
            "room",
        ]

    def date_extract(self, date: str):  # noqa
        if not date or date == "--.--.----" or type(date) != str:
            return None

        date = date.replace("Загрузка ...", "").strip()
        try:
            return datetime.datetime.strptime(date, "%d.%m.%Y").date()
        except ValueError:
            pass

        try:
            return datetime.datetime.strptime(date[:10], "%d.%m.%Y").date()
        except ValueError:
            pass

        try:
            return datetime.datetime.strptime(date.split()[0], "%d.%m.%Y").date()
        except ValueError:
            pass

        for key, value in dict_month.items():
            if key in date:
                date = date.replace(key, value)
                date = ".".join(date.split())

        date = date.split(".")

        if len(date) == 2:
            date = ".".join(["01"] + date)
            return datetime.datetime.strptime(date[:10], "%d.%m.%Y").date()

    def fillna_organization_level(  # noqa
        self, budget_level: str, full_name_customer: str, inn_customer: str
    ):
        if type(budget_level) != str:
            budget_level = None
        elif type(budget_level) == str:
            budget_level = budget_level.lower()

        if type(full_name_customer) != str:
            full_name_customer = None
        elif type(full_name_customer) == str:
            full_name_customer = full_name_customer.lower()

        if type(inn_customer) != str:
            inn_customer = None

        if budget_level:
            for list_name, name in zip(
                [list_local, list_sub, list_fed], ["местный", "субъектовый", "федеральный"]
            ):
                for name_trigger in list_name:
                    if name_trigger.lower() in budget_level:
                        return name

        # если не получилось выделить данные из budget_level
        # попробуем сделать это с full_name_customer
        if full_name_customer:
            for list_name, name in zip(
                [list_fed_2, list_local_2, list_sub_2, list_anothe],
                ["федеральный", "местный", "субъектовый", "иное"],
            ):
                for name_trigger in list_name:
                    if name_trigger.lower() in full_name_customer:
                        return name

            for list_name, name in zip(
                [list_fed_3, list_local_3, list_sub_3],
                ["федеральный", "местный", "субъектовый", "иное"],
            ):
                for name_trigger in list_name:
                    if name_trigger.lower() in full_name_customer:
                        return name

            if (
                "администрац" in full_name_customer
                or "комитет по управлению имуществом" in full_name_customer  # noqa
            ) and not all(
                [
                    i in full_name_customer
                    for i in ["моксв", "севастопол" "президент", "санкт-петербур"]
                ]
            ):
                return "местный"

            if "городская дума" in full_name_customer and "моксв" not in full_name_customer:
                return "местный"

        if inn_customer:
            for inn_dict, name in zip(
                [inn_mun, inn_sub, inn_fed, inn_another],
                ["местный", "субъектовый", "федеральный", "иное"],
            ):
                for inn in inn_dict.keys():
                    if inn == inn_customer:
                        return name
        # добавить логги

        return None

    def extract_data_from_kbk(self, kbk, year):
        dict_kbk = {
            "code_main_admin": None,
            "code_section_sub": None,
            "code_direction_expenses": None,
            "code_type_expenses": None,
            "code_national_project": None,
            "value_code_section": None,
            "value_code_sub": None,
            "value_code_type_expenses": None,
            "name_national_project": None,
            "name_fed_national_project": None,
        }
        if not kbk or type(kbk) != str:
            return dict_kbk

        if len(kbk) == 3 or kbk[:-3] == "0" * 17:
            code_type_expenses = kbk[-3:]
            value_code_type_expenses = self.kbk_type.loc[
                self.kbk_type.code == code_type_expenses, "mean"
            ].to_list()

            if len(value_code_type_expenses):
                dict_kbk["value_code_type_expenses"] = value_code_type_expenses[0]
            else:
                pass
                # логи
            dict_kbk["code_type_expenses"] = code_type_expenses
            return dict_kbk

        elif len(kbk) == 20:
            kbk_search = re.compile(r"(\S\S\S)(\S\S\S\S)(\S\S\S\S\S\S\S\S\S\S)(\S\S\S)")
            kbk_find = kbk_search.search(kbk)

            # код главного распоредителя бюджетных средств
            code_main_admin = kbk_find.group(1)
            # код раздела и подраздела
            code_section_sub = kbk_find.group(2)
            # код целевой статьи
            code_direction_expenses = kbk_find.group(3)
            # код вида расходов
            code_type_expenses = kbk_find.group(4)
            # код национального проекта
            code_national_project = (
                code_direction_expenses[3:5] if not code_direction_expenses[3].isdigit() else None
            )

            value_code_section = self.kbk_section.loc[
                (self.kbk_section.year == year) & (self.kbk_section.code == code_section_sub[:2]),
                "mean",
            ].to_list()
            if len(value_code_section):
                dict_kbk["value_code_section"] = value_code_section[0]
            else:
                pass

            value_code_sub = self.kbk_section.loc[
                (self.kbk_section.year == year) & (self.kbk_section.code == code_section_sub),
                "mean",
            ].to_list()
            if len(value_code_sub):
                dict_kbk["value_code_sub"] = value_code_sub[0]
            else:
                pass

            value_code_type_expenses = self.kbk_type.loc[
                self.kbk_type.code == code_type_expenses, "mean"
            ].to_list()
            if len(value_code_type_expenses):
                dict_kbk["value_code_type_expenses"] = value_code_type_expenses[0]
            else:
                pass

            if code_national_project:
                list_national_project = self.kbk_np.loc[
                    (self.kbk_np.year == year) & (self.kbk_np.code == code_national_project),
                    ["name_national_project", "name_fed_national_project"],
                ].values

                if len(list_national_project):
                    dict_kbk["name_national_project"] = list_national_project[0]
                    dict_kbk["name_fed_national_project"] = list_national_project[1]
                else:
                    pass
                    # логи

            dict_kbk["code_main_admin"] = code_main_admin
            dict_kbk["code_section_sub"] = code_section_sub
            dict_kbk["code_direction_expenses"] = code_direction_expenses
            dict_kbk["code_type_expenses"] = code_type_expenses
            dict_kbk["code_national_project"] = code_national_project

            return dict_kbk

        else:
            return dict_kbk
            # добавить логи

    def check_address(self, address: str, code: str, code_type: str):
        if type(code) != str or code == "":
            return address
        if type(code_type) != str or code_type == "":
            return address
        if type(address) != str:
            address = ""

        list_check = ["Российская Федерация", "РФ", "обл", "ул", "край", "г,", "п."]
        is_nan = type(address) == float
        is_telephon = address.replace("-", "").replace(" ", "").isdigit()
        is_email = ("@" in address) and not any([i in address for i in list_check])
        is_empty_string = address == ""

        need_replace = any([is_nan, is_telephon, is_email, is_empty_string])

        if not need_replace:
            return address
        else:
            unique = code + "_" + code_type
            if unique in self.cache_org_address:
                return self.cache_org_address[unique]["address"]
            else:
                # добавить логи
                return None

    def processing_date_org(self, df: Series, columns: list):
        dict_result = {}
        date_list = [i for i in columns if "date" in i]
        for date_type in date_list:
            dict_result[date_type] = self.date_extract(df[date_type])

        for date_type, prefix_addres in zip(["address_customer", "postal_address"], ["c", "pc"]):
            dict_result_address = self.address_dec.address_decompose(df[date_type])
            for key, value in dict_result_address.items():
                dict_result[f"{prefix_addres}_{key}"] = value

        dict_result["organization_level"] = self.fillna_organization_level(
            df["organization_level"], df["full_name_customer"], df["inn_customer"]
        )
        site = df["site"]
        dict_result["site"] = site if site != "http://" else None

        return pd.Series(dict_result)[columns].to_list()

    def processing_date_contract(self, df: Series, columns: list):
        dict_result = {}
        year = self.default_year_for_kbk
        date_list = [i for i in columns if "date" in i]

        for date_type in date_list:
            dict_result[date_type] = self.date_extract(df[date_type])

        for address_type, prefix_addres in zip(
            ["address_customer", "address_supplier", "postal_address_supplier"], ["c", "s", "ps"]
        ):
            address = df[address_type]
            if address_type == "address_customer":
                address = self.check_address(address, code=df["code"], code_type=df["code_type"])
                dict_result["address_customer"] = address

            dict_result_address = self.address_dec.address_decompose(address)
            for key, value in dict_result_address.items():
                dict_result[f"{prefix_addres}_{key}"] = value

        dict_result["organization_level"] = self.fillna_organization_level(
            df["budget_level"], df["full_name_customer"], df["inn_customer"]
        )

        for date in [
            "date_contract",
            "date_summarizing",
            "date_posting",
            "date_performance",
            "date_end_performance",
            "date_contract_registry",
        ]:
            if type(dict_result[date]) == datetime.date:
                year = str(dict_result[date].year)
                break

        dict_result.update(self.extract_data_from_kbk(df["kbk"], year))

        # внести изменение в парсер и удалить
        grouds_single_supplier = df["grouds_single_supplier"]
        if type(grouds_single_supplier) == str:
            dict_result["grouds_single_supplier"] = (
                df["grouds_single_supplier"].replace("Загрузка ...", "").strip()
            )
        else:
            dict_result["grouds_single_supplier"] = None

        return pd.Series(dict_result)[columns].to_list()

    def add_new_address_to_cache(self, df: DataFrame):
        df_address = df[["address_customer", "code", "code_type"]]
        for index in df_address.index:
            address = df_address.loc[index, "address_customer"]
            code = df_address.loc[index, "code"]
            code_type = df_address.loc[index, "code_type"]
            if type(code) != str or type(code_type) != str or type(address) != str:
                continue

            unique = code + "_" + code_type
            if unique not in self.cache_org_address:
                self.cache_org_address[unique] = address

                pd.DataFrame(
                    {
                        "address": [address],
                        "code": [code],
                        "code_type": [code_type],
                        "year": [self.default_year_for_kbk],
                    },
                    index=[0],
                )[self.columns_cache].to_csv(
                    self.path_cache_org_address, mode="a", index=False, header=False, sep="|"
                )

    def run_org(self, path_input: str, path_output: str):
        df = pd.read_csv(path_input, sep="|", dtype="str")
        columns = [
            "date_registration",
            "date_last_change",
            "date_registration_tax",
            "date_iky",
            "organization_level",
            "site",
        ] + [
            prefix + "_" + address_level
            for prefix in ["c", "pc"]
            for address_level in self.address_level
        ]
        df[columns] = df.apply(
            lambda x: self.processing_date_org(x, columns=columns), axis=1, result_type="expand"
        )
        self.add_new_address_to_cache(df)
        df.to_csv(path_output, sep="|", index=False)

    def run_contract(self, path_input: str, path_output: str):
        df = pd.read_csv(path_input, sep="|", dtype="str")
        columns = [
            "date_summarizing",
            "date_posting",
            "date_contract",
            "date_performance",
            "date_contract_registry",
            "date_update_registry",
            "date_start_performance",
            "date_end_performance",
            "date_registration_supplier",
            "code_main_admin",
            "code_section_sub",
            "code_direction_expenses",
            "code_type_expenses",
            "code_national_project",
            "value_code_section",
            "value_code_sub",
            "value_code_type_expenses",
            "name_national_project",
            "name_fed_national_project",
            "organization_level",
            "grouds_single_supplier",
            "address_customer",
        ] + [
            prefix + "_" + address_level
            for prefix in ["c", "s", "ps"]
            for address_level in self.address_level
        ]
        df[columns] = df.apply(
            lambda x: self.processing_date_contract(x, columns=columns),
            axis=1,
            result_type="expand",
        )
        df.to_csv(path_output, sep="|", index=False)
