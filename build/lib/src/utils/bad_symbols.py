import re


def remove_bad_symbols(string: str) -> str:
    string = re.sub("\n|\||\xa0", "", string.strip())
    return " ".join(string.split())
