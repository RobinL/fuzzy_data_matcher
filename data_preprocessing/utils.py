import fuzzy
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
    d_fn = fuzzy.DMetaphone(4)
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


def postgres_term_freqs():
    sql = """
    drop table if exists term_frequencies2;
    create table term_frequencies2 as
    select word,
    count(*) as occurrences,
    1.0000 as freq from
    (select regexp_split_to_table(upper(full_address), '[^\w]+|\s+') as word from all_addresses) as t
    where word != ''
    group by word
    order by count(*) desc;
    """