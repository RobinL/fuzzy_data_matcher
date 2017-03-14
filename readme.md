## Packages needed:

Anaconda distribution of Python (numpy, pandas)

    pip install fuzzy
    pip install python-levenshtein

You need fts4 enabled in your sqlite.dll - you might need to replace
this in your anaconda installation directory with the one found [here](http://www.sqlite.org/download.html) :

See also [this](http://stackoverflow.com/questions/3823659/how-to-setup-fts3-fts4-with-python2-7-on-windows) stackoverflow

## Basic usage instructions.

This repo provides codes that allows you to easily fuzzy match two datasets - i.e. probabalistically match two datasets based on which row in the target dataset is most likely to be a match for each candidate record.  This allows for missing information and misspellings etc.

See [this example](https://github.com/RobinL/fuzzy_data_matcher/blob/master/Simple%20demo.ipynb) for basic usage instructions.  You should be able to modify this example to your purposes.

You start with the path to the csv of the two datasets you want to match.  

By default the algorithm will attempt a fuzzy match on all fields of these datasets.  You can specify `candidate_drop_columns` and `target_drop_columns` if you want to modify this behaviour.

You can also optionally specify `candidate_dmetaphone_cols` and `target_dmetaphone_cols` if you have columns which contain text information that may have misspellings.  This may improve match rates, and will work on columns such as first name, surname, address, but not on columns such as an ID or other code.  
