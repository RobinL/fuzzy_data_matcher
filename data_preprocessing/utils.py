import metaphone
import pandas as pd
import re

def clean_and_normalise_string(my_str):
    my_str = my_str.lower()
    my_str =  re.sub("\s{2,100}", " ", my_str)
    exclude = set(".'")
    my_str = ''.join(ch for ch in my_str if ch not in exclude)
    return my_str

def get_dmetaphone(x,my_fn):
    if not pd.isnull(x):
        return my_fn(x)[0]

def add_dmetaphone_cols(df, list_of_names=[]):
    d_fn = metaphone.doublemetaphone
    for n_field in list_of_names:
        df[n_field+"_dmetaphone"] = df[n_field].apply(get_dmetaphone, args=(d_fn,))
    return df

def add_unique_id(df,prefix=""):
    df["auto_generated_row_id"] = 1
    df["auto_generated_row_id"] = df["auto_generated_row_id"].cumsum()
    df["auto_generated_row_id"] = prefix + df["auto_generated_row_id"].astype(str)
    return df

def concat_fields(df, drop_list = []):

    for r in df.iterrows():
        index = r[0]
        row = r[1]

        concats = []

        for k in row.keys():
            if k not in drop_list:
                if row[k] and not pd.isnull(row[k]):
                    concats.append(str(row[k]))

        df.loc[index, "concat_all"] = " ".join(concats)

    return df


# from future_builtins import map  # Only on Python 2

from collections import Counter
from itertools import chain



def get_freq_dict_from_col(series):
    def row_to_str_split(r):
        return r.split()

    freq_counts = Counter(chain.from_iterable(map(row_to_str_split, series)))
    total_token_count = sum(freq_counts.values())
    relative_freqs = {k: freq_counts[k]*1.0/total_token_count for k in freq_counts}
    return relative_freqs