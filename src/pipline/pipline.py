import os
from multiprocessing import Process, Manager

import pandas as pd

from src import GetNum, split_data, ParsingDataContract, extract_code_id, ParsingOrg, test_proxy


class PiplineParsing:
    def __init__(self, input_dir: str, list_proxy: list):
        self.input_dir = input_dir
        self.list_proxy = list_proxy

        self.get_num = GetNum(
            path_dir_input=f"data/data_from_spendgov/{input_dir}",
            path_name_result=f"data/contract_number/numbers/{input_dir}.csv",
            path_dir_log="logs/logs_get_num/",
        )

    def create_parsing_contract_process(self, proxy, filenames, continue_parsing):
        while filenames:
            try:
                name_file_pars = filenames.pop()
            except IndexError:
                name_file_pars = False

            if name_file_pars:
                path_df = (
                    f"data/contract_number/split_data/contract/{self.input_dir}/{name_file_pars}"
                )
                parsing_data = ParsingDataContract(
                    path_df=path_df,
                    path_output="data/raw_data/contract/",
                    path_dir_log="logs/parsing_data/",
                    path_contract_problem="data/contract_number/problem_contract/parsing_data/",
                    parser=proxy,
                    continue_parsing=continue_parsing,
                )
                parsing_data.run()

                pd.DataFrame().to_csv(f"logs/success/{self.input_dir}/contract/{name_file_pars}")

    def create_parsing_org_process(self, proxy, filenames, continue_parsing):
        while filenames:
            try:
                name_file_pars = filenames.pop()
            except IndexError:
                name_file_pars = False

            if name_file_pars:
                path_df = (
                    f"data/contract_number/split_data/code/{self.input_dir}/{name_file_pars}"
                )
                parsing_org = ParsingOrg(
                    path_df=path_df,
                    path_output="data/raw_data/org/",
                    path_dir_log="logs/parsing_org/",
                    path_contract_problem="data/contract_number/problem_contract/parsing_org",
                    parser=proxy,
                    continue_parsing=continue_parsing,
                )
                parsing_org.run()

                pd.DataFrame().to_csv(f"logs/success/{self.input_dir}/org/{name_file_pars}")

    def start_parsing_data(self, data_type: str, continue_parsing: bool):
        if data_type == "contract":
            data_type_class = self.create_parsing_contract_process
        elif data_type == "org":
            data_type_class = self.create_parsing_org_process
        else:
            print("Ошибка: Неверный аргумент data_type для start_parsing_data")
            return None

        manager = Manager()
        filenames = manager.list()
        list_files = os.listdir(f"data/contract_number/split_data/contract/{self.input_dir}/")
        if continue_parsing:
            success_files = os.listdir(f"logs/success/{self.input_dir}/{data_type}/")
            list_files = list(set(list_files) - set(success_files))
            print(
                f"Ранее обработано {len(success_files)} из {len(list_files) + len(success_files)}"
            )

        filenames.extend(list_files)
        processes = [
            Process(target=data_type_class, args=[proxy, filenames, continue_parsing])
            for proxy in self.list_proxy
        ]

        for process in processes:
            process.start()

        for process in processes:
            process.join()

    def run(self, continue_work: bool = False):
        if not os.path.exists(f"logs/success/{self.input_dir}/"):
            os.mkdir(f"logs/success/{self.input_dir}/")
        if not os.path.exists(f"logs/success/{self.input_dir}/contract/"):
            os.mkdir(f"logs/success/{self.input_dir}/contract/")
        if not os.path.exists(f"logs/success/{self.input_dir}/org/"):
            os.mkdir(f"logs/success/{self.input_dir}/org/")

        if not continue_work:
            print("Извлечение номеров контрактов и их разбивка")
            self.get_num.run()
            split_data(
                path_data=f"data/contract_number/numbers/{self.input_dir}.csv",
                path_output="data/contract_number/split_data/contract",
                n_split=500,
            )
            print("Извлечение номеров контрактов и их разбивка завершена")

        print("Начало парсинга контрактов")
        self.start_parsing_data(data_type="contract")
        print("Парсинг контрактов завершен")

        print("Извлечение кодов заказчиков и их разбивка")
        extract_code_id(
            input_path=f"data/raw_data/contract/{self.input_dir}/",
            path_global_set_code="data/global_cache/cache_code.csv",
            path_global_set_id="data/global_cache/cache_id.csv",
            output_path="data/contract_number/code_id",
        )

        split_data(
            path_data=f"data/contract_number/code_id/{self.input_dir}/",
            path_output="data/contract_number/split_data/code/",
            n_split=50,
        )
        print("Извлечение кодов заказчиков и их разбивка завершена")

        print("Начало парсинга заказчиков")
        self.start_parsing_data(data_type="org")
        print("Парсинг контрактов завершен")


def test(input_dir: str, continue_work: bool = False):
    list_proxy = pd.read_csv("data/proxy.csv", header=None, dtype=str)[0].to_list()
    good_proxy = test_proxy(list_proxy)
    print(f"Работает {len(good_proxy)} из {len(good_proxy)} прокси")

    pipline_parsing = PiplineParsing(input_dir, good_proxy)
    pipline_parsing.run(continue_work=continue_work)


if __name__ == "__main__":
    test("2018", continue_work=False)
