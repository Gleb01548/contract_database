import os

import pandas as pd


def extract_code_id(
    input_path: str, path_global_set_code: str, path_global_set_id: str, output_path: str
):
    input_path = input_path.removesuffix("/")
    output_path = output_path.removesuffix("/")

    for path in [path_global_set_code, path_global_set_id]:
        if not os.path.exists(path):
            file_name = os.path.basename(path)
            print(f"Нет кэша {file_name}. Создан пустой!")
            df = pd.DataFrame({"number": []})
            df.to_csv(path, sep="|", index=False)

    global_set_code = set(pd.read_csv(path_global_set_code, sep="|", dtype="str")["number"])
    global_set_id = set(pd.read_csv(path_global_set_id, sep="|", dtype="str")["number"])
    set_code = set()
    set_id = set()
    for name in os.listdir(input_path):
        if name.endswith(".csv"):
            df = pd.read_csv(
                os.path.join(input_path, name),
                sep="|",
                dtype="str",
                usecols=["unique_site_code", "unique_site_id"],
            )
            df.fillna("0", inplace=True)
            for col_name in ["unique_site_code", "unique_site_id"]:
                buffer_set = set(df[col_name])
                if col_name == "unique_site_code":
                    buffer_set -= global_set_code
                    buffer_set -= set(["0"])
                    set_code.update(buffer_set)
                else:
                    buffer_set -= global_set_id
                    buffer_set -= set(["0"])
                    set_id.update(buffer_set)
    list_code = list(set_code)
    list_id = list(set_id)
    list_code_type = ["Code"] * len(list_code) + ["Id"] * len(list_id)
    list_code.extend(list_id)
    df_code = pd.DataFrame({"code": list_code, "code_type": list_code_type})

    dir_name = os.path.basename(input_path)
    output_path = os.path.join(output_path, dir_name)

    if not os.path.exists(output_path):
        os.makedirs(output_path)
    df_code.to_csv(os.path.join(output_path, "code.csv"), sep="|", index=False)


if __name__ == "__main__":
    extract_code_id(
        input_path="data/raw_data/contract/2014/",
        path_global_set_code="data/global_cache/cache_code.csv",
        path_global_set_id="data/global_cache/cache_id.csv",
        output_path="data/contract_number/code_id",
    )
