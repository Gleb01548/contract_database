import os
import json
import logging
from zipfile import ZipFile

import pandas as pd
from tqdm import tqdm


class GetNum:
    """
    Класс принимает папку с архивами за определенный год, в которых содержатся
    json файлы от счетной палаты.

    Извлекаються данные: номер контракта, адрес заказчика и его инн.

    Выделенные номера и адреса заказчиков (актуальные адреса на момент заключения
    контракта спарсить нельзя)
    помещаются в один файл (path_name_result).

    Из папки куда будет помещаться файл с результатом считываются все файлы
    и формируется на основании номера контракта.
    """

    def __init__(
        self,
        path_dir_input: str,
        path_name_result: str,
        path_dir_log: str,
        drop_last: bool = True,
    ) -> None:
        """
        Params:
        - path_dir_input (str) - путь до архивов с первоначальными данными от счетной палаты
        - path_name_result (str) -путь и имя файла где буду сохранены результат работы скрипта
        - path_dir_log (str) - путь до логи
        - drop_last (bool) - удалять или нет path_name_result с по тому же пути
          и с таким же именем.
        Если не удалять, то данные будут записаны поверх него.
        """

        self.path_dir_input = path_dir_input
        self.path_name_result = path_name_result
        self.path_dir_log = path_dir_log

        self.init_logger()

        if not os.path.exists(self.path_name_result) or drop_last:
            pd.DataFrame(columns=["number_contract", "adress_customer", "inn_customer"]).to_csv(
                f"{self.path_name_result}", index=False, sep="|"
            )
        self.make_cache()

    def make_cache(self):
        """
        Метод формирует кэш для исключения повторной записи
        данных в результирующий файл
        """
        number_contract_list = []
        path_for_data = os.path.dirname(self.path_name_result)
        list_name = os.listdir(path_for_data)
        for name_file in list_name:
            df = pd.read_csv(
                os.path.join(path_for_data, name_file),
                sep="|",
                dtype="str",
                usecols=["number_contract"],
            )
            number_contract_list.extend(df["number_contract"].to_list())
            self.set_contract_num = set(number_contract_list)

    def get_full_list(self):
        """
        Получам полный список файлов и архивах и путей к ним
        """
        full_list_file = []
        zip_list = os.listdir(self.path_dir_input)

        for zip_name in zip_list:
            path_zip = os.path.join(self.path_dir_input, zip_name)
            with ZipFile(path_zip, "r") as z:
                for filename in z.namelist():
                    full_list_file.append({"zip_name": path_zip, "file_name": filename})
        return full_list_file

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

    def get_data(self, line):
        """
        Метод извлекает данные из json строки (номер контракта,
        адрес заказчика, инн заказчика)
        """
        number_contract = line.get("regNum")
        if number_contract is None:
            self.count_none_contract += 1
            self.logger.info(
                f"Файл: {self.file_name} | Номер line: {self.count_lin} | regNum is None"
            )
            return None

        elif number_contract in self.set_contract_num:
            self.count_duplicated += 1
        else:
            self.set_contract_num.add(number_contract)
            self.count_not_duplicated += 1

            customer_data = line.get("customer")

            if customer_data is not None:
                adress_customer = customer_data.get("postalAddress")
                inn_customer = customer_data.get("inn")

                pd.DataFrame(
                    {
                        "number_contract": [number_contract],
                        "adress_customer": [adress_customer],
                        "inn_customer": [inn_customer],
                    },
                ).to_csv(self.path_name_result, mode="a", index=False, header=False, sep="|")

    def run(self):
        self.logger_print.info("СТАРТ ПРОГРАММЫ")
        self.count_not_duplicated = 0  # считает не дубликаты
        self.count_lin = -1  # счетчик строк
        self.count_duplicated = 0  # считает дубликаты
        # считает случаи когда при обращении к словарю
        # за номером контракта возращается None
        self.count_none_contract = 0

        full_list_file = self.get_full_list()

        for dict_info in tqdm(full_list_file):
            zip_file_path = dict_info["zip_name"]
            file_name = dict_info["file_name"]
            self.file_name = file_name

            zip_name = os.path.dirname(zip_file_path)
            self.logger.info(f"Имя архива: {zip_name} | Имя файла в архиве: {file_name}")
            # Файл отдельно на жесткий диск не распаковываем
            with ZipFile(zip_file_path, "r") as z:
                for line in z.open(file_name, "r"):
                    if line is not None:
                        self.count_lin += 1
                        self.get_data(json.loads(line))

        self.logger_print.info(f"Число уникальных номеров: {self.count_not_duplicated}")
        self.logger_print.info(f"Число не пустых строк: {self.count_lin}")
        self.logger_print.info(f"Число дубликатов: {self.count_duplicated}")
        self.logger_print.info(f"Число none-номеров: {self.count_none_contract}")


def test():
    get_num = GetNum(
        path_dir_input="data/data_from_spendgov/2014",
        path_name_result="data/contract_number/test_data/2014.csv",
        path_dir_log="logs/logs_get_num",
        drop_last=True,
    )
    get_num.run()


if __name__ == "__main__":
    test()
