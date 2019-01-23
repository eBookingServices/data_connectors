from sys import getsizeof
from textwrap import shorten


def convert_col_types(column):
    """

    :param column: Expected input
    {
        'colname': 'listing_id',
        'type': 'bigint',
        'nullable': 'YES',
        'key': '',
        'default': None,
        'extra': '',
        'size': 20
    }
    :return:
    """
    red_column = {"colname": column["colname"], "size": -1, "precision": -1}

    if column['type'] =='tinyint':
        red_column["type"] = "smallint"
    elif column["type"] == "smallint":
        red_column["type"] = "int"
    elif column["type"] in ["bigint", "int"]:
        red_column["type"] = "numeric"
        red_column["size"] = 20
        red_column["precision"] = 0
    elif column["type"] in ['double', "decimal", "float"]:
        red_column["type"] = "double precision"
        red_column["precision"] = column["precision"]

    elif column["type"] in ['enum', "set"]:
        red_column["type"] = 'varchar'
        red_column["size"] = 1019

    elif column["type"] in ["text", "mediumtext", "varchar", "char"]:
        red_column["type"] = 'varchar'
        red_column["size"] = 65535

    elif column["type"] in ["timestamp", "datetime"]:
        red_column["type"] = "timestamp"
    elif column["type"] == "date":
        red_column["type"] = "date"
    else:
        raise ValueError("Unknown MySQL type {}, "
                         "column name is {}".format(column["type"],
                                                    column["colname"]))

    return red_column


def truncate_strings(val, max_size=65535):
    if len(val)*4 <= max_size:
        return val

    string_ = val
    cnt = len(string_)
    ge = getsizeof

    for _ in range(cnt-1, 0, -1):
        if ge(string_[0:_].encode("utf-8")) <= max_size:
            return string_[0:_]


def redshift_formatter(row):
    isi = isinstance
    tr = truncate_strings
    return tuple([tr(r.replace("\x00", "")) if isi(r, str)  else r for r in row])

