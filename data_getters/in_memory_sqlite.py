from data_getters.data_getter_abc import DataGetterABC
import pandas as pd

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
from data_matcher import Record


class DataGetter_Memory(DataGetterABC):

    def __init__(self, con, freq_dict):
        """
        Take the targets df, make the freq table
        and make an in memory sqlite db with fts enabled
        """

        self.target_con = con
        self.freq_dict = freq_dict
        self.max_results = 100

        self.get_records_sql = """
            SELECT * FROM fts_target WHERE fts_target MATCH '{}' limit {};
            """

    def df_to_record_objects(self,df):
        records_list= df.to_dict(orient="records")

        return_list = []

        for r in records_list:

            record = Record(r["concat_all"], r["auto_generated_row_id"])

            return_list.append(record)

        return return_list


    def get_freq(self,term):
        """
        Given a token, returns the relative frequency with which it appears in the corpus
        For instance, if 'road' appears in one out of a hundred words, would return 0.01
        if passed term = 'road'

        Returns a float.  Returns None if token is not found
        """

        try:
            return self.freq_dict[term]
        except:
            return None

    def get_potential_matches_from_record(self, record):
        """
        Given an address object, returns a list of potential matches.

        Returns an dictionary of address objects; the keys are the unique
        ids of the address e.g. a UPRN
        """

        #Query against the con to the sqlite database
        def get_potential_matches(sub_tokens):


            search_tokens = " ".join(sub_tokens)

            SQL = self.get_records_sql.format(search_tokens, self.max_results)
            # logger.debug(SQL)

            df = pd.read_sql(SQL,self.target_con)

            return df


        tokens_orig = record.tokens
        tokens_ordered = record.tokens_specific_to_general_by_freq
        return_list = []

        for tokens in [tokens_orig, tokens_ordered]:
            for i in range(len(tokens)):

                sub_tokens = tokens[i:]
                if len(sub_tokens)<3:
                    df= pd.DataFrame()
                    break

                df = get_potential_matches(sub_tokens)


                if len(df)>0 and len(df)<self.max_results:
                    return_list.extend(self.df_to_record_objects(df))
                    break

        return return_list