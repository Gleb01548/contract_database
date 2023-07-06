import os
import math

import pandas as pd


def split_data(
    path_data: str, n_split: int, path_output: str = None, attachment: str = None
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
    if path_output is None:
        path_output = os.path.join(
            "data/contract_number/split_data_test",
            os.path.basename(path_data).removesuffix(".csv"),
        )
    if not os.path.exists(path_output):
        os.makedirs(path_output)

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


if __name__ == "__main__":
    split_data(path_data="data/contract_number/test_data/2014.csv", n_split=500)
