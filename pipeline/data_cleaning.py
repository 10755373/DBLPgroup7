import pyspark.sql.functions as fn
import nltk
from nltk.stem import WordNetLemmatizer
from nltk import word_tokenize, pos_tag
import re
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType

# download nltk libraries
nltk.download('punkt')
nltk.download('wordnet')

# clean data by removing characters 
def remove_some_chars(col_name):
    removed_chars = ("!", "?", '[', ']', '{', '}', '(', ')', '.', '*', '+', '=', '-', '_', "'", ':', ';', '<', '>', "''", '@', '#')
    regexp = "|".join('\{0}'.format(i) for i in removed_chars)
    return fn.regexp_replace(col_name, regexp, " ")

# remove non-alphabatical characters and non-numbers
def remove_non_alphabatical(sdf):
    sdf = sdf.withColumn("author_cleaned", fn.regexp_replace("pauthor", "[^0-9a-zA-Z]+", " "))
    sdf = sdf.withColumn("title_cleaned", fn.regexp_replace("ptitle", "[^0-9a-zA-Z]+", " "))
    return sdf

# clean certain columns
def cleanup(sdf):
    for col_name in ['pauthor', 'ptitle', 'journal_full', 'journal']:
        sdf = sdf.withColumn(col_name, remove_some_chars(col_name))
        sdf = sdf.withColumn(col_name, fn.lower(col(col_name)))
        sdf = sdf.withColumn(col_name, fn.trim(col(col_name)))
    return sdf

# make year values absolute
def clean_year(sdf):
    sdf = sdf.withColumn('year',fn.ceil(fn.abs(sdf.pyear)))
    sdf = sdf.drop('pyear')
    return sdf

# call clean up functions together
def clean_data(sdf):
    sdf = clean_year(sdf)
    sdf = cleanup(sdf)
    sdf = remove_non_alphabatical(sdf)
    return sdf
