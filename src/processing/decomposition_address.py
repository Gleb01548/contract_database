import os

import pandas as pd
from pullenti.address.AddressService import AddressService


class DecompositionAddress:
    def __init__(self, path_for_cache: str, year: str):
        self.path_for_cache = path_for_cache
        self.columns = [
            "country",
            "regioncity",
            "regionarea",
            "district",
            "settlement",
            "city",
            "citydistrict",
            "locality",
            "territory",
            "street",
            "plot",
            "building",
            "apartment",
            "room",
            "coef",
        ]
        self.extra_columns = ["address", "year"]
        self.year = year

        if not os.path.exists(path_for_cache):
            pd.DataFrame(columns=self.extra_columns + self.columns).to_csv(
                path_for_cache, sep="|", index=False
            )

        self.dict_cahce = pd.read_csv(
            path_for_cache,
            sep="|",
            dtype="str",
            index_col="address",
            usecols=self.columns + [self.extra_columns[0]],
        ).to_dict(orient="index")

    def address_decompose(self, address: str):
        if not address or address == "" or type(address) != str:
            return {key: None for key in self.columns}

        if address in self.dict_cahce:
            return self.dict_cahce[address]

        else:
            return self.use_pullenti(address)

    def use_pullenti(self, address: str):
        dict_res = {key: None for key in self.columns}
        process_address = AddressService.process_single_address_text(address)

        dict_res["coef"] = process_address.coef

        for address_element in process_address.items:
            level = str(address_element.level).split(".")[1].lower()
            element_address = address_element.to_string_min()
            dict_res[level] = element_address

        self.add_address_to_cache(address, dict_res)
        dict_res.pop("coef")
        return dict_res

    def add_address_to_cache(self, address, dict_result):
        self.dict_cahce[address] = dict_result
        dict_result_for_df = dict_result.copy()
        dict_result_for_df["address"] = address
        dict_result_for_df["year"] = self.year
        pd.DataFrame(dict_result_for_df, index=[0])[self.extra_columns + self.columns].to_csv(
            self.path_for_cache, sep="|", index=False, mode="a", header=False
        )
