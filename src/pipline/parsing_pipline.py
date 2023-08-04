from src import GetNum, split_data, ParsingDataContract, extract_code_id, ParsingOrg


def start(input_dir: str):
    get_num = GetNum(
        path_dir_input=f"data/data_from_spendgov/{input_dir}",
        path_name_result=f"data/contract_number/numbers/{input_dir}.csv",
        path_dir_log="logs/logs_get_num/",
    )
    get_num.run()

    split_data(
        path_data=f"data/contract_number/numbers/{input_dir}.csv",
        path_output="data/contract_number/split_data/contract",
        n_split=500,
    )

    parsing_data = ParsingDataContract(
        path_df=f"data/contract_number/split_data/contract/{input_dir}/1.csv",
        path_output="data/raw_data/contract/",
        path_dir_log="logs/parsing_data/",
        path_contract_problem="data/contract_number/problem_contract/parsing_data/",
    )
    parsing_data.run()

    extract_code_id(
        input_path=f"data/raw_data/contract/{input_dir}/",
        path_global_set_code="data/global_cache/cache_code.csv",
        path_global_set_id="data/global_cache/cache_id.csv",
        output_path="data/contract_number/code_id",
    )

    split_data(
        path_data=f"data/contract_number/code_id/{input_dir}/",
        path_output="data/contract_number/split_data/code/",
        n_split=500,
    )

    parsing_org = ParsingOrg(
        path_df="data/contract_number/split_data/code/{input_dir}/1.csv",
        path_output="data/raw_data/org",
        path_dir_log="logs/parsing_org",
        path_contract_problem="data/contract_number/problem_contract/parsing_org",
        continue_parsing=True,
    )
    parsing_org.run()


def test(input_dir: str):
    start(input_dir)


if __name__ == "__main__":
    test("2021")
