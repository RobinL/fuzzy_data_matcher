from data_preprocessing.utils import  clean_and_normalise_string
import math
from functools import reduce
class Record(object):

    """
    The address object holds addresses, whether they be the candidate address (the address for which we are trying
    to find a match)or the potential matches (the list of addresses which may match the candidate)
    """

    def __init__(self,concat_record = None, record_id= None, data_getter=None): #If it's given a freq_conn it will use it to order tokens

        self.record_id = record_id
        self.data_getter = data_getter
        self.concat_record = None
        if concat_record:
            self.tokens_original_order = self.tokenise(concat_record)
            self.set_ordered_tokens_freq()
        self.match_score = None
        self.match_probability = None


    def set_ordered_tokens_freq(self):

        """
        Order the tokens by their term frequency using the connection
        """

        if self.data_getter:
            tokens_to_score = list(set(self.tokens_original_order))
            scored_tokens = [{"token" : t, "score": self.data_getter.get_freq(t)} for t in tokens_to_score]
            scored_tokens.sort(key=lambda x: x["score"] if x["score"] else 0)
            scored_tokens = [t["token"] for t in scored_tokens if t["score"]] #Only keep tokens if they exist in abp!!
            self.tokens_specific_to_general_by_freq = scored_tokens


    def tokenise(self,concat_record):
        """
        Tokenizes an address into its words
        """
        concat_record = clean_and_normalise_string(concat_record)
        return concat_record.split()

    def __repr__(self):
        return "Record object: {}".format(" ".join( self.tokens_original_order))

from operator import mul
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
        self.potential_target_matches = self.data_getter.get_potential_matches_from_record(self.candidate_record)


    def get_prob_from_target_token(self, target_token, candidate_address_tokens):
        """
        Computes a score that represents the 'distinctness' or 'discriminativeness' of this token
        - i.e. how much it helps in narrowing down the full list of all addresses.
        A score of 1 means that this token doesn't narrow down the full list at all.  A score of 0.01
        means it cuts it down by 99 per cent etc.
        Note this is not a true probability because if the token cannot be found in the candidate address,
        the code attempst to find a fuzzy match.  The probability here is ill defined.
        Note that the eventual score will not be a true probability because it does not take into account any correlations
        between term frequencies (i.e. it is similar to naive bayes).
        """

        if target_token in candidate_address_tokens:
            prob = self.data_getter.get_freq(target_token)

        else:
            prob = 1

        return prob


    def set_prob_on_potential_target_match(self, target_record):

        target_tokens = target_record.tokens_original_order
        candidate_tokens = self.candidate_record.tokens_original_order
        individual_scores = []

        for t in target_tokens:
            p = self.get_prob_from_target_token(t, candidate_tokens)
            individual_scores.append(p)

        target_record.match_probability = reduce(mul, individual_scores, 1)


        target_record.match_score =  (math.log10(target_record.match_probability) * -1) / 30


    def find_match(self):
        """
        Look through the potential matches to find the one which
        is most likely to be a match
        """

        if len(self.potential_target_matches) == 0:
            self.best_match = Record()
            return

        for record in self.potential_target_matches:
            self.set_prob_on_potential_target_match(record)

        self.potential_matches = sorted(self.potential_target_matches, key=lambda x: x.match_score, reverse=True)
        self.best_match = self.potential_matches[0]