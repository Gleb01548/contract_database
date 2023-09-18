import os

import pandas as pd
from tqdm import tqdm

from src.constants import PATH_RAW_DATA_CONTRACT, PATH_CODE_ID


def extract_code_id(input_path: str, path_global_set_code_id: str, output_path: str):
    if not os.path.exists(path_global_set_code_id):
        file_name = os.path.basename(path_global_set_code_id)
        print(f"Нет кэша {file_name}. Создан пустой!")
        pd.DataFrame(columns=["code", "code_type"]).to_csv(
            path_global_set_code_id, sep="|", index=False
        )
    df = pd.read_csv(path_global_set_code_id, sep="|", dtype="str")
    global_set_code = set(df.loc[df["code_type"] == "Code", "code"])
    global_set_id = set(df.loc[df["code_type"] == "Id", "code"])
    list_result_code = []
    list_result_type_code = []
    for name in tqdm(os.listdir(input_path)):
        if name.endswith(".csv"):
            df = pd.read_csv(
                os.path.join(input_path, name),
                sep="|",
                dtype="str",
                usecols=["code", "code_type"],
                low_memory=False,
            )
            # добавить проверку на code
            set_code = set(df.loc[df["code_type"] == "Code", "code"]) - global_set_code
            set_id = set(df.loc[df["code_type"] == "Id", "code"]) - global_set_id

            global_set_code = global_set_code.union(set_code)
            global_set_id = global_set_id.union(set_id)

            list_result_code.extend(list(set_code))
            list_result_type_code.extend(["Code"] * len(set_code))

            list_result_code.extend(list(set_id))
            list_result_type_code.extend(["Id"] * len(set_id))

    df_code = pd.DataFrame({"code": list_result_code, "code_type": list_result_type_code})
    df_code.to_csv(os.path.join(output_path), sep="|", index=False)


def test(input_dir: str):
    input_path = os.path.join(PATH_RAW_DATA_CONTRACT, input_dir)
    output_path = os.path.join(PATH_CODE_ID, f"{input_dir}.csv")

    extract_code_id(
        input_path=input_path, output_path=output_path, path_global_set_code_id=PATH_CODE_ID
    )


if __name__ == "__main__":
    test("2015")
