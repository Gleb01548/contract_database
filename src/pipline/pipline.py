import os
import shutil
from multiprocessing import Process, Manager

import pandas as pd
from tqdm import tqdm

from src import (
    GetNum,
    split_data,
    ParsingDataContract,
    extract_code_id,
    ParsingOrg,
    test_proxy,
    make_logger,
    ProcessingData,
    make_cache_code_id,
)
from src.constants import (
    PATH_LOGS_PIPLINE,
    PATH_LOGS_GET_NUM,
    PATH_LOGS_PARSING_CONTRACT,
    PATH_LOGS_PARSING_ORG,
    PATH_LOGS_PROCESSING,
    PATH_LOGS_SUCСESS_CONTRACT,
    PATH_LOGS_SUCСESS_ORG,
    PATH_LOGS_SUCСESS_GET_NUM,
    PATH_LOGS_SUCСESS_EXTRACT_CODE,
    PATH_LOGS_SUCСESS_PROCESSING_CONTRACT,
    PATH_LOGS_SUCСESS_PROCESSING_ORG,
    PATH_LOGS_PROBLEM_CONTRACT,
    PATH_LOGS_PROBLEM_ORG,
    PATH_DATA_FROM_SPENDGOV,
    PATH_NUMBERS,
    PATH_CODE_ID,
    PATH_SPLIT_DATA_CONTRACT,
    PATH_SPLIT_DATA_CODE,
    PATH_RAW_DATA_CONTRACT,
    PATH_RAW_DATA_ORG,
    PATH_PROCESSING_DATA_CONTRACT,
    PATH_PROCESSING_DATA_ORG,
    PATH_CACHE_CODE_ID,
    PATH_CACHE_ADDRESS,
    PATH_CACHE_ORG_ADDRESS,
    PATH_LOGS_SUCCESS_MAKE_CAHCE_CODE_ID,
    PATH_PROXY_LIST,
    PATH_KBK_TABLE,
)


class PiplineParsing:
    def __init__(self, input_dir: str, use_proxy: bool = True, continue_work: bool = True):
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
        self.path_logs_processing = os.path.join(PATH_LOGS_PROCESSING, self.input_dir)
        self.path_logs_sucess_contract = os.path.join(PATH_LOGS_SUCСESS_CONTRACT, self.input_dir)
        self.path_logs_sucess_org = os.path.join(PATH_LOGS_SUCСESS_ORG, self.input_dir)
        self.path_logs_sucess_get_num = os.path.join(PATH_LOGS_SUCСESS_GET_NUM, self.input_dir)
        self.path_logs_sucess_extract_code = os.path.join(
            PATH_LOGS_SUCСESS_EXTRACT_CODE, self.input_dir
        )
        self.path_logs_success_processing_contract = os.path.join(
            PATH_LOGS_SUCСESS_PROCESSING_CONTRACT, self.input_dir
        )
        self.path_logs_sucess_processing_org = os.path.join(
            PATH_LOGS_SUCСESS_PROCESSING_ORG, self.input_dir
        )
        self.path_logs_sucess_make_cache_code_id = os.path.join(
            PATH_LOGS_SUCCESS_MAKE_CAHCE_CODE_ID, self.input_dir
        )
        self.path_logs_problem_contract = os.path.join(PATH_LOGS_PROBLEM_CONTRACT, self.input_dir)
        self.path_logs_problem_org = os.path.join(PATH_LOGS_PROBLEM_ORG, self.input_dir)

        # для считывания и сохранения файлов (не логи)
        self.path_data_from_spendgov = os.path.join(PATH_DATA_FROM_SPENDGOV, self.input_dir)
        self.path_numbers = os.path.join(PATH_NUMBERS, f"{self.input_dir}.csv")
        self.path_code_id = os.path.join(PATH_CODE_ID, f"{self.input_dir}.csv")
        self.path_split_data_contract = os.path.join(PATH_SPLIT_DATA_CONTRACT, self.input_dir)
        self.path_split_data_code = os.path.join(PATH_SPLIT_DATA_CODE, self.input_dir)
        self.path_raw_data_contract = os.path.join(PATH_RAW_DATA_CONTRACT, self.input_dir)
        self.path_raw_data_org = os.path.join(PATH_RAW_DATA_ORG, self.input_dir)
        self.path_processing_data_contract = os.path.join(
            PATH_PROCESSING_DATA_CONTRACT, self.input_dir
        )
        self.path_processing_data_org = os.path.join(PATH_PROCESSING_DATA_ORG, self.input_dir)

        # создаем папки для логги
        self.creat_dir_if_not_exist(PATH_LOGS_PIPLINE)
        self.creat_dir_if_not_exist(PATH_LOGS_GET_NUM)
        self.creat_dir_if_not_exist(self.path_logs_contract)
        self.creat_dir_if_not_exist(self.path_logs_org)

        for path in [
            # создаем папки по логи
            self.path_logs_sucess_get_num,
            self.path_logs_sucess_contract,
            self.path_logs_sucess_extract_code,
            self.path_logs_sucess_org,
            self.path_logs_success_processing_contract,
            self.path_logs_sucess_processing_org,
            self.path_logs_sucess_make_cache_code_id,
            self.path_logs_problem_contract,
            self.path_logs_problem_org,
            # создаем папки под результаты программы
            self.path_split_data_contract,
            self.path_split_data_code,
            self.path_raw_data_contract,
            self.path_raw_data_org,
            self.path_processing_data_contract,
            self.path_processing_data_org,
        ]:
            self.creat_dir_if_not_exist(path, necessarily=self.necessarily_make_dir)

        # создаем логгеры
        self.logger_print, self.logger = make_logger(self.path_logs_pipline)

        # создаем доп. пути для файлов
        self.path_logs_sucess_get_num = os.path.join(self.path_logs_sucess_get_num, "succes.csv")
        self.path_logs_sucess_extract_code = os.path.join(
            self.path_logs_sucess_extract_code, "succes.csv"
        )
        self.path_logs_sucess_make_cache_code_id = os.path.join(
            self.path_logs_sucess_make_cache_code_id, "succes.csv"
        )

    def creat_dir_if_not_exist(self, path: str, necessarily: bool = False):
        if not os.path.exists(path):
            os.mkdir(path)
        elif necessarily:
            shutil.rmtree(path)
            os.mkdir(path)

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
            path_success = os.path.join(self.path_logs_sucess_contract, name_file_pars)

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
            pd.DataFrame().to_csv(path_success)

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
            path_success = os.path.join(self.path_logs_sucess_org, name_file_pars)

            parsing_org = ParsingOrg(
                path_df=path_code_id,
                path_output=path_output,
                path_log=path_logs,
                path_org_problem=path_org_problem,
                proxy=proxy,
                continue_parsing=continue_parsing,
            )
            parsing_org.run()

            pd.DataFrame().to_csv(path_success)

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

    def processing_data(self, continue_parsing):
        list_raw_org = os.listdir(self.path_raw_data_org)
        list_raw_contract = os.listdir(self.path_raw_data_contract)

        if continue_parsing:
            sucess_org = os.listdir(self.path_logs_sucess_processing_org)
            sucess_contract = os.listdir(self.path_logs_success_processing_contract)

            list_raw_org = list(set(list_raw_org) - set(sucess_org))
            list_raw_contract = list(set(list_raw_contract) - set(sucess_contract))

            print(
                f"Ранее обработано org: {len(sucess_org)} из {len(sucess_org) + len(list_raw_org)}"  # noqa
            )
            print(
                f"Ранее обработано contract: {len(sucess_contract)} из {len(sucess_contract) + len(list_raw_contract)}"  # noqa
            )

            if not list_raw_org and not list_raw_contract:
                return None

        list_raw_org = sorted(list_raw_org, key=lambda x: int(x.removesuffix(".csv")))
        list_raw_contract = sorted(list_raw_contract, key=lambda x: int(x.removesuffix(".csv")))

        processing_data = ProcessingData(
            path_cache_address=PATH_CACHE_ADDRESS,
            path_cache_org_address=PATH_CACHE_ORG_ADDRESS,
            path_kbk_table=PATH_KBK_TABLE,
            default_year_for_kbk=self.input_dir,
            path_log=self.path_logs_processing,
        )

        for path_file_input in tqdm(list_raw_org):
            path_success = os.path.join(self.path_logs_sucess_processing_org, path_file_input)
            path_file_output = os.path.join(self.path_processing_data_org, path_file_input)
            path_file_input = os.path.join(self.path_raw_data_org, path_file_input)

            processing_data.run_org(path_input=path_file_input, path_output=path_file_output)
            pd.DataFrame().to_csv(path_success)

        for path_file_input in tqdm(list_raw_contract):
            path_success = os.path.join(
                self.path_logs_success_processing_contract, path_file_input
            )
            path_file_output = os.path.join(self.path_processing_data_contract, path_file_input)
            path_file_input = os.path.join(self.path_raw_data_contract, path_file_input)

            processing_data.run_contract(path_input=path_file_input, path_output=path_file_output)
            pd.DataFrame().to_csv(path_success)

    def run(self):
        if not os.path.exists(self.path_logs_sucess_get_num):
            self.get_num = GetNum(
                path_dir_input=self.path_data_from_spendgov,
                path_name_result=self.path_numbers,
                path_log=self.path_logs_get_num,
            )
            self.logger_print.info("Извлечение номеров контрактов и их разбивка")
            self.get_num.run()
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
                path_global_set_code_id=PATH_CACHE_CODE_ID,
                output_path=self.path_code_id,
            )

            split_data(
                path_data=self.path_code_id,
                path_output=self.path_split_data_code,
                n_split=60,
            )
            pd.DataFrame().to_csv(self.path_logs_sucess_extract_code)

        self.logger_print.info("Извлечение кодов заказчиков и их разбивка завершена")

        self.logger_print.info("Начало парсинга заказчиков")
        self.start_parsing_data(data_type="org", continue_parsing=self.continue_work)
        self.logger_print.info("Парсинг заказчиков завершен")

        self.logger_print.info("Начало обработки данных о контрактах и заказчиках")
        self.processing_data(continue_parsing=self.continue_work)
        self.logger_print.info("Обработка данных о контрактах и заказчиках завершена")

        self.logger_print.info("Формирование кэша поставщиков")

        if not os.path.exists(self.path_logs_sucess_make_cache_code_id):
            make_cache_code_id(self.path_processing_data_org, PATH_CACHE_CODE_ID)
            pd.DataFrame().to_csv(self.path_logs_sucess_make_cache_code_id)
        self.logger_print.info("Формирование кэша поставщиков завершено")
        self.logger_print.info("Выполнение программы успешно завершено!")


def test(input_dir: str, continue_work: bool = False):
    pipline_parsing = PiplineParsing(input_dir, continue_work=continue_work)
    pipline_parsing.run()


if __name__ == "__main__":
    test("2016", continue_work=True)
