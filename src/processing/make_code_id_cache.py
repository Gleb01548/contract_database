import os

import pandas as pd
from tqdm import tqdm

from src.constants import PATH_CACHE_CODE_ID, PATH_PROCESSING_DATA_ORG


def make_cache_code_id(path_input_dir: str, path_cahce: str):
    if not os.path.exists(path_cahce):
        pd.DataFrame(columns=["code", "code_type"]).to_csv(path_cahce, sep="|", index=False)

    df = pd.read_csv(path_cahce, sep="|", dtype=str)
    columns = list(df.columns)
    cache_id = set(df.loc[df["code_type"] == "Code", "code"])
    cache_code = set(df.loc[df["code_type"] == "Id", "code"])

    for file_name in tqdm(
        sorted(os.listdir(path_input_dir), key=lambda x: int(x.removesuffix(".csv")))
    ):
        file_name = os.path.join(path_input_dir, file_name)

        df = pd.read_csv(file_name, sep="|", dtype=str, usecols=["code", "code_type"])

        for index in range(len(df)):
            code = df.loc[index, "code"]
            code_type = df.loc[index, "code_type"]

            if code not in cache_id or code not in cache_code:
                pd.DataFrame({"code": [code], "code_type": [code_type]}, index=[0])[
                    columns
                ].to_csv(path_cahce, sep="|", index=False, header=False, mode="a")

                if code_type == "Code":
                    cache_code.add(code)
                elif code_type == "Id":
                    cache_id.add(code)
                else:
                    raise ValueError(f"Неверное значение code_type {code_type}")


if __name__ == "__main__":
    name = "2014"
    path_dir_input = os.path.join(PATH_PROCESSING_DATA_ORG, name)
    make_cache_code_id(path_dir_input, PATH_CACHE_CODE_ID)
