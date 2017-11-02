from data_preprocessing.utils import add_dmetaphone_cols, add_unique_id, concat_fields, get_freq_dict_from_col
import pandas as pd
from data_preprocessing.utils import clean_and_normalise_string
from sqlalchemy import create_engine
from data_getters.in_memory_sqlite import DataGetter_Memory
from data_matcher import Record, Matcher

def get_candidate_target_datasets(df_candidate, 
           df_target, 
           match_cols_candidate = None, 
           match_cols_target = None, 
           dmetaphone_cols_candidate = None, 
           dmetaphone_cols_target = None,):
    
    # If match_cols_candidate and match_cols_target are blank, match on all columns with the same name 
    if match_cols_candidate is None and match_cols_target is None:
        common_columns = set(df_candidate.columns)
        common_columns = common_columns.union(df_target.columns)
        match_cols_candidate = list(common_columns)
        match_cols_target = list(common_columns)

    if dmetaphone_cols_candidate:
        df_candidate = add_dmetaphone_cols(df_candidate, dmetaphone_cols_candidate)
        dmetaphone_cols_candidate = [c + "_dmetaphone" for c in dmetaphone_cols_candidate]
        match_cols_candidate.extend(dmetaphone_cols_candidate)

    if dmetaphone_cols_target:
        df_target = add_dmetaphone_cols(df_target, dmetaphone_cols_target)
        dmetaphone_cols_target = [c + "_dmetaphone" for c in dmetaphone_cols_target]
        match_cols_target.extend(dmetaphone_cols_target) 
    
    #TODO:  Another improvement at the cost of computational complexity here would be to take n grams of letter combinations in longer words.  
    # This will effectively allow for partial matches, and mean that we might pick up additional misspellings.

    # Add a column called "auto_generated_row_id" to each dataset so we can uniquely identify any record
    df_candidate = add_unique_id(df_candidate, "candidate_")
    df_target = add_unique_id(df_target, "target_")

    df_candidate = concat_fields(df_candidate, fields = match_cols_candidate)
    df_target = concat_fields(df_target, fields = match_cols_target)

    df_target["concat_all"] = df_target["concat_all"].apply(clean_and_normalise_string)
    df_candidate["concat_all"] = df_candidate["concat_all"].apply(clean_and_normalise_string)

    return df_candidate, df_target


def write_target_db_to_memory_and_get_connection(df_target_for_db):

    engine = create_engine('sqlite://')
    con = engine.connect()
    df_target_for_db.to_sql("target", engine, index=False)
    con.execute("CREATE VIRTUAL TABLE fts_target USING fts4(auto_generated_row_id TEXT, concat_all TEXT);")
    con.execute("INSERT INTO fts_target SELECT * FROM target")
    return con



def match_candidate_target_datasets(df_c, df_t):

    # Make sure we're working with copies rather than modifying original objects
    df_c = df_c[["auto_generated_row_id", "concat_all"]].copy()
    df_t = df_t[["auto_generated_row_id", "concat_all"]].copy()

    def get_matching_record(row):
        get_matching_record.counter +=1

        #TODO:  Add logging here to keep track of % progress

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


def fuzzy_left_join(df_candidate, 
           df_target, 
           match_cols_candidate = None, 
           match_cols_target = None, 
           dmetaphone_cols_candidate = None, 
           dmetaphone_cols_target = None,
           search_intensity = 500,
           max_matches_returned_per_record = 1
           ):
    
    #TODO:  Add support for max_matches_returned_per_record > 1

    df_c_orig_cols = df_candidate.columns
    df_t_orig_cols = df_target.columns

    df_candidate, df_target = get_candidate_target_datasets(df_candidate, 
           df_target = df_target, 
           match_cols_candidate = match_cols_candidate, 
           match_cols_target = match_cols_target, 
           dmetaphone_cols_candidate = dmetaphone_cols_candidate, 
           dmetaphone_cols_target = dmetaphone_cols_target)
    
    df_matched = match_candidate_target_datasets(df_candidate, df_target)
    df_matched = df_matched.drop(['concat_all_can', 'concat_all_tar'], axis=1)
    # In the results we give back to the user, we want to first present the match score, then the match columns side by side, then other columns, 
    # We will assume that the user gives the target and candidate match cols in the same order

    # First of all create a dataset will all candidate records, left joined to target
    df_all = df_matched.merge(df_candidate, left_on = "auto_generated_row_id_can", right_on = "auto_generated_row_id", how = "left", suffixes = ("_mtc", "_cdt"))
    df_all = df_all.drop(["auto_generated_row_id_can", "auto_generated_row_id"], axis=1)

    df_all = df_all.merge(df_target, left_on = "auto_generated_row_id_tar", right_on = "auto_generated_row_id", how = "left", suffixes = ("_mtc", "_tgt"))
    df_all = df_all.drop(["auto_generated_row_id_tar", "auto_generated_row_id"], axis=1)

    retain_columns = [c for c in df_all.columns if "dmetaphone" not in c and "concat_all" not in c]
    df_all = df_all[retain_columns]

    #auto_generated_row_id_can	auto_generated_row_id_tar	concat_all_can	concat_all_tar	match_probability	match_score

    return df_all







