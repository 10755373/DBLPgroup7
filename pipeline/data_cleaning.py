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

# function to check whether author is correctly placed in pauthor column or not
def check_author(sdf):
    authorsdf1 = sdf.withColumn('wordCount_author', fn.size(fn.split(fn.col('pauthor'), ' ')))
    authorsdf1 = authorsdf1.withColumn('aut1_bool', fn.when((authorsdf1.pauthor.contains(r'|') | (fn.col('wordCount_author') < 5)), 1).otherwise(0))
    authorsdf1 = authorsdf1.withColumn('aut_bool', fn.when((fn.col('aut1_bool') == 1), 1).otherwise(0))   
    authorsdf1 = authorsdf1.drop('aut1_bool')
    return authorsdf1

# swap author if needed
def swap_author(sdf):
    bc = sdf.withColumn('author', fn.when(fn.col('aut_bool') == 1, fn.col('pauthor')).otherwise(fn.col('ptitle')))
    return bc

# swap title if needed
def swap_title(sdf):
    bc = sdf.withColumn('title', fn.when(fn.col('aut_bool') == 1, fn.col('ptitle')).otherwise(fn.col('pauthor')))
    return bc

# function to call for swap functions
def swap_values(sdf):
    sdf = swap_author(sdf)
    sdf = swap_title(sdf)
    return sdf









