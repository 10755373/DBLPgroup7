import pyspark.sql.functions as fn
import nltk
from nltk.stem import WordNetLemmatizer
from nltk import word_tokenize, pos_tag

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

# clean certain columns
def cleanup(sdf):
    for col_name in ['pauthor', 'ptitle', 'journal_full', 'journal']:
        sdf = sdf.withColumn(col_name, remove_some_chars(col_name))
        sdf = sdf.withColumn(col_name, fn.lower(col(col_name)))
        sdf = sdf.withColumn(col_name, fn.trim(col(col_name)))
    return sdf

# call function to lemmatize values
def lemmatize(sdf):
    for col_name in ['ptitle', 'journal_full', 'journal']:
        # sdf = sdf.withColumn(col_name, lemmatizer(col_name.cast(fn.StringType()))) 
        sdf = sdf.withColumn(col_name, lemmatizer(sdf.col_name.cast(fn.StringType()))) 

    return sdf

# make year values absolute
def clean_year(sdf):
    sdf = sdf.withColumn('year',fn.ceil(fn.abs(sdf.pyear)))
    sdf = sdf.drop('pyear')
    return sdf

# lemmatize strings
def lemmatizer(data_str):
    # expects a string
    # data_str = sdf.fn.col('ptitle')
    list_pos = 0
    cleaned_str = ''
    lmtzr = WordNetLemmatizer() 
    text = data_str.split() 
    tagged_words = pos_tag(text) 
    for word in tagged_words:
        if 'v' in word[1].lower():
            lemma = lmtzr.lemmatize(word[0], pos='v')
        else:
            lemma = lmtzr.lemmatize(word[0], pos='n')
        if list_pos == 0: 
            cleaned_str = lemma
        else:
            cleaned_str = cleaned_str + ' ' + lemma
        list_pos += 1 
    return cleaned_str

# call clean up functions together
def clean_data(sdf):
    sdf = clean_year(sdf)
    sdf = cleanup(sdf)
    # sdf = lemmatize(sdf)
    return sdf
