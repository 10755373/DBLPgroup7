from curses import def_prog_mode
<<<<<<< HEAD
import pyspark.sql.functions as fn
import nltk
from nltk.stem import WordNetLemmatizer
from nltk import word_tokenize, pos_tag
=======
from pyspark.sql.functions import * 
import nltk
from nltk.stem import WordNetLemmatizer
from nltk import word_tokenize 
>>>>>>> 41ac0d1fe7d5490eae779142c59d85a879a8f767

from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType



nltk.download('punkt')
nltk.download('wordnet')

<<<<<<< HEAD
def remove_some_chars(col_name):
    removed_chars = ("!", "?", '[', ']', '{', '}', '(', ')', '.', '*', '+', '=', '-', '_', "'", ':', ';', '<', '>', "''", '@', '#')
    regexp = "|".join('\{0}'.format(i) for i in removed_chars)
    return fn.regexp_replace(col_name, regexp, " ")


def cleanup(sdf):
    for col_name in ['pauthor', 'ptitle', 'journal_full', 'journal']:
        sdf = sdf.withColumn(col_name, remove_some_chars(col_name))
        sdf = sdf.withColumn(col_name, fn.lower(col(col_name)))
        sdf = sdf.withColumn(col_name, fn.trim(col(col_name)))
    return sdf

def lemmatize(sdf):
    for col_name in ['ptitle', 'journal_full', 'journal']:
        # sdf = sdf.withColumn(col_name, lemmatizer(col_name.cast(fn.StringType()))) 
        sdf = sdf.withColumn(col_name, lemmatizer(sdf.col_name.cast(fn.StringType()))) 

    return sdf



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

def clean_data(sdf):
    sdf = clean_year(sdf)
    sdf = cleanup(sdf)
    # sdf = lemmatize(sdf)
=======
def swap_author_title(sdf):
    sdf = sdf.withColumn('wordCount_author', size(split(col('pauthor'), ' ')))
    sdf = sdf.select('*',
          when(check_swap(sdf.wordCount_author, sdf.pauthor) == 'author', [sdf.pauthor, sdf.ptitle])
          .when(check_swap(sdf.wordCount_author, sdf.pauthor) == 'title', [sdf.ptitle, sdf.pauthor])
          .otherwise(None).alias(['author_new', 'title_new'])
          .drop(['pauthor', 'ptitle', 'wordCount_author']))
    
    return sdf

def check_swap(wordCount_author, pauthor):
    # the or-condition isn't working, don't know why; will check again on Wednesday.
    if (array_contains(pauthor, r'|')) | (wordCount_author < 5):
        return 'author'
    else:
        return 'title'

def remove_some_chars(col_name):
    removed_chars = ("!", "?", '[', ']', '{', '}', '(', ')', '.', '*', '+', '=', '-', '_', "'", ':', ';', '<', '>', "''")
    regexp = "|".join('\{0}'.format(i) for i in removed_chars)
    return regexp_replace(col_name, regexp, "")


def cleanup(sdf):
    for col_name in ['pauthor', 'ptitle', 'journal_full', 'journal']:
        sdf = sdf.withColumn(col_name, remove_some_chars(col_name))
        sdf = sdf.withColumn(col_name, lower(col(col_name)))
        sdf = sdf.withColumn(col_name, trim(col(col_name)))
>>>>>>> 41ac0d1fe7d5490eae779142c59d85a879a8f767
    return sdf

def check_author(sdf):
    authorsdf1 = sdf.withColumn('wordCount_author', fn.size(fn.split(fn.col('pauthor'), ' ')))
    authorsdf1 = authorsdf1.withColumn('aut1_bool', fn.when((authorsdf1.pauthor.contains(r'|') | (fn.col('wordCount_author') < 5)), 1).otherwise(0))
    authorsdf1 = authorsdf1.withColumn('aut_bool', fn.when((fn.col('aut1_bool') == 1), 1).otherwise(0))   
    authorsdf1 = authorsdf1.drop('aut1_bool')
    return authorsdf1


def swap_author(sdf):
    bc = sdf.withColumn('author', fn.when(fn.col('aut_bool') == 1, fn.col('pauthor')).otherwise(fn.col('ptitle')))
    return bc


def swap_title(sdf):
    bc = sdf.withColumn('title', fn.when(fn.col('aut_bool') == 1, fn.col('ptitle')).otherwise(fn.col('pauthor')))
    return bc


<<<<<<< HEAD
def swap_values(sdf):
    sdf = swap_author(sdf)
    sdf = swap_title(sdf)
    return sdf


def clean_year(sdf):
    sdf = sdf.withColumn('year',fn.ceil(fn.abs(sdf.pyear)))
    sdf = sdf.drop('pyear')
    return sdf

=======
def clean_year(sdf):
    sdf = sdf.withColumn('pyear',ceil(abs(sdf.pyear)))
    return sdf


def clean_data(sdf):
    sdf = cleanup(sdf)
    sdf = clean_year(sdf)
    # sdf = tokenize_titles(sdf)
    # sdf = regextokenize_titles(sdf)
    # sdf = tokenize_author(sdf)
    # sdf = regextokenize_author(sdf)
    # sdf = swap_author_title(sdf)
    return sdf

# these functions I made when I was trying to lemmatize the words, will have a look again on Wednesday
# def tokenize_titles(sdf):
#     tokenizer = Tokenizer(inputCol="ptitle", outputCol="title_words")
#     tokenized = tokenizer.transform(sdf)
#     return tokenized

# def regextokenize_titles(sdf):
#     regexTokenizer = RegexTokenizer(inputCol="ptitle", outputCol="title_char", pattern="\\w")
#     tokenized = regexTokenizer.transform(sdf)
#     return tokenized

# def tokenize_author(sdf):
#     tokenizer = Tokenizer(inputCol="pauthor", outputCol="author_words")
#     tokenized = tokenizer.transform(sdf)
#     return tokenized

# def regextokenize_author(sdf):
#     regexTokenizer = RegexTokenizer(inputCol="pauthor", outputCol="author_char", pattern="\\w")
#     tokenized = regexTokenizer.transform(sdf)
#     return tokenized
>>>>>>> 41ac0d1fe7d5490eae779142c59d85a879a8f767




