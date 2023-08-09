import requests

import pandas as pd


def test_proxy(list_proxy: list):
    good_proxy = []
    for proxy in list_proxy:
        proxy = {"http": f"http://{proxy}"}

        url = "https://www.google.com/"

        try:
            response = requests.get(url, proxies=proxy)

            if response.ok:
                good_proxy.append(proxy)

        except requests.exceptions.RequestException:
            continue
    return good_proxy


def test(path_proxy: str):
    list_proxy = pd.read_csv(path_proxy, header=None, dtype=str)[0].to_list()
    good_proxy = test_proxy(list_proxy)

    print(f"Работает {len(good_proxy)} из {len(good_proxy)} прокси")


if __name__ == "__main__":
    test("data/proxy.csv")
