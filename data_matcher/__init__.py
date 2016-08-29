from data_preprocessing.utils import  clean_and_normalise_string

class Record(object):

    """
    The address object holds addresses, whether they be the candidate address (the address for which we are trying
    to find a match)or the potential matches (the list of addresses which may match the candidate)
    """

    def __init__(self,concat_record, record_id, data_getter=None): #If it's given a freq_conn it will use it to order tokens

        self.record_id = record_id
        self.data_getter = data_getter
        self.tokens = self.tokenise(concat_record)
        self.set_ordered_tokens_freq()
        self.match_score = 0
        self.relative_score = 0

    def set_ordered_tokens_freq(self):

        """
        Order the tokens by their term frequency using the connection
        """

        if self.data_getter:
            tokens_to_score = list(set(self.tokens))
            scored_tokens = [{"token" : t, "score": self.data_getter.get_freq(t)} for t in tokens_to_score]
            scored_tokens.sort(key=lambda x: x["score"])
            scored_tokens = [t["token"] for t in scored_tokens if t["score"]] #Only keep tokens if they exist in abp!!
            self.tokens_specific_to_general_by_freq = scored_tokens


    def tokenise(self,concat_record):
        """
        Tokenizes an address into its words
        """
        concat_record = clean_and_normalise_string(concat_record)
        return concat_record.split()


class Matcher(object):

    """
    The matcher stores the candidate address and the list of potential matches
    and contains the core algorithms that perform the match and compute
    scores on the match

    Note that it doesn't deal with obtaining the list of potential matches.
    This is delegated to a 'data_getter' class - which will be specific
    to the application.
    """

    def __init__(self, data_getter ,candidate_record):

        self.data_getter = data_getter
        self.candidate_record = candidate_record