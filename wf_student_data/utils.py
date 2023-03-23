import pandas as pd
import numpy as np
import re

INT_RE = re.compile(r'[0-9]+')

def to_datetime(object):
    try:
        datetime = pd.to_datetime(object, utc=True).to_pydatetime()
        if pd.isnull(datetime):
            date = None
    except:
        datetime = None
    return datetime

def to_date(object):
    try:
        date = pd.to_datetime(object).date()
        if pd.isnull(date):
            date = None
    except:
        date = None
    return date

def to_singleton(object):
    try:
        num_elements = len(object)
        if num_elements > 1:
            raise ValueError('More than one element in object. Conversion to singleton failed')
        if num_elements == 0:
            return None
        return object[0]
    except:
        return object

def to_boolean(object):
    if isinstance(object, bool):
        return object
    if isinstance(object, str):
        if object in ['True', 'true', 'TRUE', 'T']:
            return True
        if object in ['False', 'false', 'FALSE', 'F']:
            return False
        return None
    if isinstance(object, int):
        if object == 1:
            return True
        if object == 0:
            return False
        return None
    return None

def extract_alphanumeric(object):
    if pd.isna(object):
        return None
    try:
        object_string = str(object)
    except:
        return None
    alphanumeric_string = ''.join(ch for ch in object_string if ch.isalnum())
    return alphanumeric_string

def extract_int(object):
    if pd.isna(object):
        return None
    try:
        object_string = str(object)
    except:
        return None
    m = INT_RE.search(object_string)
    if m:
        return pd.to_numeric(m[0]).astype('int')
    else:
        return None

def filter_dataframe(
    dataframe,
    filter_dict=None
):
    if filter_dict is None:
        return dataframe
    index_columns = dataframe.index.names
    dataframe=dataframe.reset_index()
    for key, value_list in filter_dict.items():
        dataframe = dataframe.loc[dataframe[key].isin(value_list)]
    dataframe.set_index(index_columns, inplace=True)
    return dataframe

def select_from_dataframe(
    dataframe,
    select_dict=None
):
    if select_dict is None:
        return dataframe
    keys, values = zip(*select_dict.items())
    for level, value in select_dict.items():
        dataframe = select_index_level(
            dataframe,
            value=value,
            level=level
        )
    return dataframe

def select_index_level(
    dataframe,
    value,
    level
):
    dataframe = (
        dataframe
        .loc[dataframe.index.get_level_values(level) == value]
        .reset_index(level=level, drop=True)
    )
    return dataframe
