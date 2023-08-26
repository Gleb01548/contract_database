import os
import math

import pandas as pd

from src.constants import (
    PATH_CODE_ID,
    PATH_SPLIT_DATA_CODE,
)


def split_data(
    path_data: str, n_split: int, path_output: str = None, attachment: str = ""
) -> None:
    """
    Метод разбивает df (path_data) на заданное
    количество примерно равных частей (n_split) и сохраняет
    по указанному пути (path_output)

    Params:
    path_data (str): путь к DataFrame который надо сплитить
    n_split (int): число сплитов на которые надо разбить файл
    path_output (str): путь к папке в которой будут сохранены данные
            если путь не задан, будет сформирован на основании path_data

    """
    df = pd.read_csv(path_data, sep="|", dtype="str")
    x_len = len(df)

    step = math.ceil(x_len / n_split)
    list_range = list(range(0, x_len, step))
    list_range.append(x_len)

    list_period = [(in_f, out_f) for (in_f, out_f) in zip(list_range, list_range[1:])]

    if attachment:
        attachment += "_"

        for index, period in enumerate(list_period):
            p0 = period[0]
            p1 = period[1]
            df[p0:p1].to_csv(f"{path_output}/{attachment}{index}.csv", sep="|", index=False)
    else:
        for index, period in enumerate(list_period):
            p0 = period[0]
            p1 = period[1]
            df[p0:p1].to_csv(f"{path_output}/{index}.csv", sep="|", index=False)


def test(input_file: str):
    path_data = os.path.join(PATH_CODE_ID, f"{input_file}.csv")
    path_output = os.path.join(PATH_SPLIT_DATA_CODE, input_file)
    split_data(path_data=path_data, path_output=path_output, n_split=10)


if __name__ == "__main__":
    test("2014")
