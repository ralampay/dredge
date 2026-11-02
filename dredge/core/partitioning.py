"""Stable directory partition recognition shared by discovery and selection."""
from datetime import date
import re


def group_key(key: str, style: str):
    directories = key.split("/")[:-1]
    normalized, partitions = [], []
    index = 0
    while index < len(directories):
        part = directories[index]
        if style != "none" and re.fullmatch(r"[^=]+=[^=]+", part):
            name, _ = part.split("=", 1)
            normalized.append(name + "={value}")
            partitions.append({"name": name, "type": "string", "layout": "hive", "segment": index})
        elif style == "auto" and re.fullmatch(r"\d{4}-\d{2}-\d{2}", part) and valid_date(part):
            normalized.append("{date}")
            partitions.append({"name": "date", "type": "date", "layout": "YYYY-MM-DD", "segment": index})
        elif (style == "auto" and index + 2 < len(directories)
              and re.fullmatch(r"\d{4}/\d{2}/\d{2}", "/".join(directories[index:index + 3]))
              and valid_date("-".join(directories[index:index + 3]))):
            normalized.extend(["{year}", "{month}", "{day}"])
            for offset, name in enumerate(("year", "month", "day")):
                partitions.append({"name": name, "type": "string", "layout": "YYYY/MM/DD", "segment": index + offset})
            index += 2
        else:
            normalized.append(part)
        index += 1
    return "/".join(normalized), partitions


def valid_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
        return True
    except ValueError:
        return False
