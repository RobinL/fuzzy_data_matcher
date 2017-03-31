from data_preprocessing.utils import add_dmetaphone_cols, add_unique_id, concat_fields,get_freq_dict_from_col
import pandas as pd
from data_preprocessing.utils import clean_and_normalise_string
from sqlalchemy import create_engine
from data_getters.in_memory_sqlite import DataGetter_Memory
from data_matcher import Record, Matcher


def get_candidate_target_datasets(candidate_path, target_path,
                                  candidate_dmetaphone_cols = None,
                                  target_dmetaphone_cols = None,
                                  candidate_drop_columns = [],
                                  target_drop_columns = []):

    df_candidate = pd.read_csv(candidate_path, encoding="utf-8", dtype=object)
    df_target = pd.read_csv(target_path, encoding="utf-8", dtype=object)

    if candidate_dmetaphone_cols:
        df_candidate = add_dmetaphone_cols(df_candidate, candidate_dmetaphone_cols)

    if target_dmetaphone_cols:
        df_target = add_dmetaphone_cols(df_target, target_dmetaphone_cols)

    df_candidate = add_unique_id(df_candidate, "candidate_")
    df_target = add_unique_id(df_target, "target_")

    candidate_drop_columns.append("auto_generated_row_id")
    target_drop_columns.append("auto_generated_row_id")
    df_target = concat_fields(df_target, drop_list=candidate_drop_columns)
    df_candidate = concat_fields(df_candidate, drop_list=target_drop_columns)

    df_target["concat_all"] = df_target["concat_all"].apply(clean_and_normalise_string)
    df_candidate["concat_all"] = df_candidate["concat_all"].apply(clean_and_normalise_string)

    df_target = df_target[[c for c in df_target.columns if "_dmetaphone" not in c]]
    df_candidate = df_candidate[[c for c in df_candidate.columns if "_dmetaphone" not in c]]

    return df_candidate, df_target

def write_target_db_to_memory_and_get_connection(df_target_for_db):

    engine = create_engine('sqlite://')
    con = engine.connect()
    df_target_for_db.to_sql("target", engine, index=False)
    con.execute("CREATE VIRTUAL TABLE fts_target USING fts4(auto_generated_row_id TEXT, concat_all TEXT);")
    con.execute("INSERT INTO fts_target SELECT * FROM target")
    return con



def match_candidate_target_datasets(df_c, df_t):

    df_c = df_c[["auto_generated_row_id", "concat_all"]].copy()
    df_t = df_t[["auto_generated_row_id", "concat_all"]].copy()

    def get_matching_record(row):
        get_matching_record.counter +=1
        if get_matching_record.counter % 250 == 0:
            print(get_matching_record.counter)
        record = Record(row["concat_all"], row["auto_generated_row_id"], datagetter)
        m = Matcher(datagetter, record)
        m.find_match()
        return pd.Series({"target_row_id": m.best_match.record_id, "match_score": m.best_match.match_score, "match_probability": m.best_match.match_probability})

    get_matching_record.counter = 0

    con = write_target_db_to_memory_and_get_connection(df_t)
    freq_dict = get_freq_dict_from_col(df_t["concat_all"])
    datagetter = DataGetter_Memory(con, freq_dict)


    matches = df_c.apply(get_matching_record, axis=1)

    df_c = pd.concat([df_c, matches], axis=1)
    df_c = df_c[["auto_generated_row_id", "target_row_id", "match_score", "match_probability", "concat_all"]]



    df_matched =  df_c.merge(df_t, left_on="target_row_id", right_on="auto_generated_row_id", how="left",
                  suffixes=["_can", "_tar"])

    df_matched= df_matched.drop("target_row_id", axis=1)

    cols = sorted(df_matched.columns)
    return df_matched[cols]

