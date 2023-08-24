import os
import shutil
from multiprocessing import Process, Manager

import logging
import pandas as pd
from logging import Logger

from src import (
    GetNum,
    split_data,
    ParsingDataContract,
    extract_code_id,
    ParsingOrg,
    test_proxy,
)

from src.constants import (
    PATH_LOGS_PIPLINE,
    PATH_LOGS_GET_NUM,
    PATH_LOGS_PARSING_CONTRACT,
    PATH_LOGS_PARSING_ORG,
    PATH_LOGS_SUCESS_CONTRACT,
    PATH_LOGS_SUCESS_ORG,
    PATH_LOGS_SUCESS_GET_NUM,
    PATH_LOGS_SUCESS_EXTRACT_CODE,
    PATH_LOGS_PROBLEM_CONTRACT,
    PATH_LOGS_PROBLEM_ORG,
    PATH_DATA_FROM_SPENDGOV,
    PATH_NUMBERS,
    PATH_CODE_ID,
    PATH_SPLIT_DATA_CONTRACT,
    PATH_SPLIT_DATA_CODE,
    PATH_RAW_DATA_CONTRACT,
    PATH_RAW_DATA_ORG,
    PATH_CACHE_CODE,
    PATH_CACHE_ID,
    PATH_CACHE_ADRESS,
    PATH_PROXY_LIST,
)


class PiplineParsing:
    def __init__(self, input_dir: str, use_proxy: bool = True, continue_work: bool = False):
        self.input_dir = input_dir
        self.use_proxy = use_proxy
        self.continue_work = continue_work

        self.initialize()
        self.load_check_proxy()

    def initialize(self):
        # определяем обязательность пересоздания папок (удаление прошлых результатов работы
        # программы)
        self.necessarily_make_dir = not self.continue_work

        # пути для логгеров
        self.path_logs_pipline = os.path.join(PATH_LOGS_PIPLINE, f"pipline_{self.input_dir}.log")
        self.path_logs_get_num = os.path.join(PATH_LOGS_GET_NUM, f"{self.input_dir}.log")
        self.path_logs_contract = os.path.join(PATH_LOGS_PARSING_CONTRACT, self.input_dir)
        self.path_logs_org = os.path.join(PATH_LOGS_PARSING_ORG, self.input_dir)
        self.path_logs_sucess_contract = os.path.join(PATH_LOGS_SUCESS_CONTRACT, self.input_dir)
        self.path_logs_sucess_org = os.path.join(PATH_LOGS_SUCESS_ORG, self.input_dir)
        self.path_logs_sucess_get_num = os.path.join(PATH_LOGS_SUCESS_GET_NUM, self.input_dir)
        self.path_logs_sucess_extract_code = os.path.join(
            PATH_LOGS_SUCESS_EXTRACT_CODE, self.input_dir
        )
        self.path_logs_problem_contract = os.path.join(PATH_LOGS_PROBLEM_CONTRACT, self.input_dir)
        self.path_logs_problem_org = os.path.join(PATH_LOGS_PROBLEM_ORG, self.input_dir)

        # для считывания и сохранения файлов
        self.path_data_from_spendgov = os.path.join(PATH_DATA_FROM_SPENDGOV, self.input_dir)
        self.path_numbers = os.path.join(PATH_NUMBERS, f"{self.input_dir}.csv")
        self.path_code_id = os.path.join(PATH_CODE_ID, f"{self.input_dir}.csv")
        self.path_split_data_contract = os.path.join(PATH_SPLIT_DATA_CONTRACT, self.input_dir)
        self.path_split_data_code = os.path.join(PATH_SPLIT_DATA_CODE, self.input_dir)
        self.path_raw_data_contract = os.path.join(PATH_RAW_DATA_CONTRACT, self.input_dir)
        self.path_raw_data_org = os.path.join(PATH_RAW_DATA_ORG, self.input_dir)

        # пути в кэшу
        self.path_cache_code = PATH_CACHE_CODE
        self.path_cache_id = PATH_CACHE_ID
        self.path_cache_adress = PATH_CACHE_ADRESS

        # создаем папки для логги
        self.creat_dir_if_not_exist(PATH_LOGS_PIPLINE)
        self.creat_dir_if_not_exist(PATH_LOGS_GET_NUM)
        self.creat_dir_if_not_exist(self.path_logs_contract)
        self.creat_dir_if_not_exist(self.path_logs_org)
        self.creat_dir_if_not_exist(
            self.path_logs_sucess_get_num, necessarily=self.necessarily_make_dir
        )
        self.creat_dir_if_not_exist(
            self.path_logs_sucess_contract, necessarily=self.necessarily_make_dir
        )
        self.creat_dir_if_not_exist(
            self.path_logs_sucess_extract_code, necessarily=self.necessarily_make_dir
        )
        self.creat_dir_if_not_exist(
            self.path_logs_sucess_org, necessarily=self.necessarily_make_dir
        )

        self.creat_dir_if_not_exist(self.path_logs_problem_contract)
        self.creat_dir_if_not_exist(self.path_logs_problem_org)

        # создаем папки под результаты программы
        self.creat_dir_if_not_exist(
            self.path_split_data_contract, necessarily=self.necessarily_make_dir
        )
        self.creat_dir_if_not_exist(
            self.path_split_data_code, necessarily=self.necessarily_make_dir
        )
        self.creat_dir_if_not_exist(
            self.path_raw_data_contract, necessarily=self.necessarily_make_dir
        )
        self.creat_dir_if_not_exist(self.path_raw_data_org, necessarily=self.necessarily_make_dir)

        # создаем логгеры
        self.logger_print, self.logger = self.make_logger(self.path_logs_pipline)

        # создаем доп. пути для файлов
        self.path_logs_sucess_get_num = os.path.join(self.path_logs_sucess_get_num, "succes.csv")
        self.path_logs_sucess_extract_code = os.path.join(
            self.path_logs_sucess_extract_code, "succes.csv"
        )

        if not self.continue_work:
            self.get_num = GetNum(
                path_dir_input=self.path_data_from_spendgov,
                path_name_result=self.path_numbers,
                path_log=self.path_logs_get_num,
            )

    def make_logger(self, path_for_file_log: str) -> tuple[Logger]:
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

    def creat_dir_if_not_exist(self, path: str, necessarily: bool = False):
        if not os.path.exists(path):
            os.mkdir(path)
        elif necessarily:
            os.remove(path)
            shutil.rmtree(path)

    def load_check_proxy(self):
        list_proxy = pd.read_csv(PATH_PROXY_LIST, header=None, dtype=str)[0].to_list()
        self.good_proxy = test_proxy(list_proxy)
        self.logger_print.info(f"Работает {len(self.good_proxy)} из {len(list_proxy)} прокси")

    def create_parsing_contract_process(self, proxy, filenames, continue_parsing):
        while filenames:
            try:
                name_file_pars = filenames.pop()
            except IndexError:
                return None

            # формируем пути для работы парсера
            path_numbers_contract = os.path.join(self.path_split_data_contract, name_file_pars)
            path_output = os.path.join(self.path_raw_data_contract, name_file_pars)
            path_logs = os.path.join(
                self.path_logs_contract, name_file_pars.replace(".csv", ".log")
            )
            path_contract_problem = os.path.join(self.path_logs_problem_contract, name_file_pars)
            path_succes = os.path.join(self.path_logs_sucess_contract, name_file_pars)

            parsing_contract = ParsingDataContract(
                path_df=path_numbers_contract,
                path_output=path_output,
                path_log=path_logs,
                path_contract_problem=path_contract_problem,
                proxy=proxy,
                continue_parsing=continue_parsing,
            )
            parsing_contract.run()

            # указываем, что файл успешно спарсен
            pd.DataFrame().to_csv(path_succes)

    def create_parsing_org_process(self, proxy, filenames, continue_parsing):
        while filenames:
            try:
                name_file_pars = filenames.pop()
            except IndexError:
                return None

            path_code_id = os.path.join(self.path_split_data_code, name_file_pars)
            path_output = os.path.join(self.path_raw_data_org, name_file_pars)
            path_logs = os.path.join(self.path_logs_org, name_file_pars.replace(".csv", ".log"))
            path_org_problem = os.path.join(self.path_logs_problem_org, name_file_pars)
            path_succes = os.path.join(self.path_logs_sucess_org, name_file_pars)

            parsing_org = ParsingOrg(
                path_df=path_code_id,
                path_output=path_output,
                path_log=path_logs,
                path_org_problem=path_org_problem,
                proxy=proxy,
                continue_parsing=continue_parsing,
            )
            parsing_org.run()

            pd.DataFrame().to_csv(path_succes)

    def start_parsing_data(self, data_type: str, continue_parsing: bool):
        if data_type == "contract":
            data_type_class = self.create_parsing_contract_process
            path_data = self.path_split_data_contract
            path_success = self.path_logs_sucess_contract

        elif data_type == "org":
            data_type_class = self.create_parsing_org_process
            path_data = self.path_split_data_code
            path_success = self.path_logs_sucess_org
        else:
            print("Ошибка: Неверный аргумент data_type для start_parsing_data")
            return None

        manager = Manager()
        filenames = manager.list()
        list_files = os.listdir(path_data)
        if continue_parsing:
            success_files = os.listdir(path_success)
            list_files = list(set(list_files) - set(success_files))
            print(
                f"Ранее обработано {len(success_files)} из {len(list_files) + len(success_files)}"
            )

        list_files = sorted(list_files, reverse=True, key=lambda x: int(x.removesuffix(".csv")))
        filenames.extend(list_files)
        processes = [
            Process(target=data_type_class, args=[proxy, filenames, continue_parsing])
            for proxy in self.good_proxy
        ]

        for process in processes:
            process.start()

        for process in processes:
            process.join()

    def run(self):
        if not os.path.exists(self.path_logs_sucess_get_num):
            self.logger_print.info("Извлечение номеров контрактов и их разбивка")
            self.get_num.run()
            print(self.path_numbers)
            print(self.path_split_data_contract)
            split_data(
                path_data=self.path_numbers,
                path_output=self.path_split_data_contract,
                n_split=500,
            )
            self.logger_print.info("Извлечение номеров контрактов и их разбивка завершена")
            del self.get_num
            pd.DataFrame().to_csv(self.path_logs_sucess_get_num)

        self.logger_print.info("Начало парсинга контрактов")
        self.start_parsing_data(data_type="contract", continue_parsing=self.continue_work)
        self.logger_print.info("Парсинг контрактов завершен")

        self.logger_print.info("Извлечение кодов заказчиков и их разбивка")

        if not os.path.exists(self.path_logs_sucess_extract_code):
            extract_code_id(
                input_path=self.path_raw_data_contract,
                path_global_set_code=self.path_cache_code,
                path_global_set_id=self.path_cache_id,
                output_path=self.path_code_id,
            )

            split_data(
                path_data=self.path_code_id,
                path_output=self.path_split_data_code,
                n_split=50,
            )
            pd.DataFrame().to_csv(self.path_logs_sucess_extract_code)

        self.logger_print.info("Извлечение кодов заказчиков и их разбивка завершена")

        self.logger_print.info("Начало парсинга заказчиков")
        self.start_parsing_data(data_type="org", continue_parsing=self.continue_work)
        self.logger_print.info("Парсинг заказчиков завершен")


def test(input_dir: str, continue_work: bool = False):
    pipline_parsing = PiplineParsing(input_dir, continue_work=continue_work)
    pipline_parsing.run()


if __name__ == "__main__":
    test("2014", continue_work=False)
