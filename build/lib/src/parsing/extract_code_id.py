import os

import pandas as pd

from src.constants import PATH_RAW_DATA_CONTRACT, PATH_CODE_ID, PATH_CACHE_CODE, PATH_CACHE_ID


def extract_code_id(
    input_path: str, path_global_set_code: str, path_global_set_id: str, output_path: str
):
    for path in [path_global_set_code, path_global_set_id]:
        if not os.path.exists(path):
            file_name = os.path.basename(path)
            print(f"Нет кэша {file_name}. Создан пустой!")
            df = pd.DataFrame({"number": []})
            df.to_csv(path, sep="|", index=False)

    global_set_code = set(pd.read_csv(path_global_set_code, sep="|", dtype="str")["number"])
    global_set_id = set(pd.read_csv(path_global_set_id, sep="|", dtype="str")["number"])
    list_result_code = []
    list_result_type_code = []
    for name in os.listdir(input_path):
        if name.endswith(".csv"):
            df = pd.read_csv(
                os.path.join(input_path, name),
                sep="|",
                dtype="str",
                usecols=["code", "code_type"],
            )
            for index in range(len(df)):
                code = df.loc[index, "code"]
                code_type = df.loc[index, "code_type"]

                if code_type == "Code" and code not in global_set_code:
                    list_result_code.append(code)
                    list_result_type_code.append("Code")
                    global_set_code.add(code)
                elif code_type == "Id" and code not in global_set_id:
                    list_result_code.append(code)
                    list_result_type_code.append("Id")
                    global_set_id.add(code)

    df_code = pd.DataFrame({"code": list_result_code, "code_type": list_result_type_code})
    df_code.to_csv(os.path.join(output_path), sep="|", index=False)


def test(input_dir: str):
    input_path = os.path.join(PATH_RAW_DATA_CONTRACT, input_dir)
    output_path = os.path.join(PATH_CODE_ID, f"{input_dir}.csv")

    extract_code_id(
        input_path=input_path,
        output_path=output_path,
        path_global_set_code=PATH_CACHE_CODE,
        path_global_set_id=PATH_CACHE_ID,
    )


if __name__ == "__main__":
    test("2014")
