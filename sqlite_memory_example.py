from data_preprocessing.utils import add_dmetaphone_cols, add_unique_id, concat_fields

import pandas as pd
from data_getters.in_memory_sqlite import DataGetter_Memory
from sqlalchemy import create_engine

from future_builtins import map

from collections import Counter
from itertools import chain

from data_matcher import Record, Matcher

from data_preprocessing.utils import clean_and_normalise_string

import re

path_to_candidate_dataset = r"test_data/dataset1/candidate_dataset.csv"
path_to_target_dataset = r"test_data/dataset1/target_dataset.csv"

df_candidate = pd.read_csv(path_to_candidate_dataset, encoding="utf-8",  dtype=object)
df_target = pd.read_csv(path_to_target_dataset, encoding="utf-8",  dtype=object)

df_candidate = add_dmetaphone_cols(df_candidate, ["first_name", "surname", "city"])
df_target = add_dmetaphone_cols(df_target, ["first_name", "surname", "city"])

df_candidate = add_unique_id(df_candidate, "candidate_")
df_target = add_unique_id(df_target, "target_")

df_target = concat_fields(df_target, drop_list=["code","auto_generated_row_id"])
df_candidate = concat_fields(df_candidate, drop_list=["code","auto_generated_row_id"])

df_target_for_db = df_target[["auto_generated_row_id", "concat_all"]]
df_candidate_for_db = df_candidate[["auto_generated_row_id", "concat_all"]]

df_target_for_db["concat_all"] = df_target_for_db["concat_all"].apply(clean_and_normalise_string)
df_candidate_for_db["concat_all"] = df_candidate_for_db["concat_all"].apply(clean_and_normalise_string)

engine = create_engine('sqlite://')
con = engine.connect()
df_target_for_db.to_sql("target", engine, index=False)
con.execute("CREATE VIRTUAL TABLE fts_target USING fts4(auto_generated_row_id TEXT, concat_all TEXT);")
con.execute("INSERT INTO fts_target SELECT * FROM target")


results = con.execute("select concat_all from target")

def row_to_str_split(r):
    my_str = r[0]
    return my_str.split()

freq_count_dict = Counter(chain.from_iterable(map(row_to_str_split, results)))
total_token_count = sum(freq_count_dict.values())
freq_count_perc = {k: freq_count_dict[k]*1.0/total_token_count for k in freq_count_dict}

# term_frequencies = pd.DataFrame(freq_counts.items(), columns=['term', 'occurrences'])
# term_frequencies = term_frequencies.sort("occurrences", ascending=False)
# total = term_frequencies["occurrences"].sum()
# term_frequencies["freq"] = term_frequencies["occurrences"]/total
# term_frequencies.to_sql("term_frequencies", engine, index=False)

datagetter_sqlite_memory = DataGetter_Memory(con, freq_count_perc)

# Get first record and attempt match
candidate_record = Record(df_candidate.loc[0,"concat_all"], df_candidate.loc[0,"auto_generated_row_id"], datagetter_sqlite_memory)


matcher = Matcher(datagetter_sqlite_memory, candidate_record)

a = 1
