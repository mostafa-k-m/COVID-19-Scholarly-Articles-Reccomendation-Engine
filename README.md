```python
import os
os.environ["JAVA_HOME"] = "/usr/lib64/jvm/jre-1.8.0-openjdk"
from pyspark.sql import SparkSession
home =  os.path.expanduser("~")
spark = SparkSession \
    .builder \
    .appName("Project4 - COVID-19") \
    .config("spark.driver.memory", "15g") \
    .config('spark.driver.maxResultSize', '15g') \
    .getOrCreate()
```

## Step 1: Read the data (the json files and metadata)

### Read the metadata file


```python
metadata = spark.read\
            .format("csv")\
            .option("header", "true")\
            .load(home+"/Dataset/metadata.csv")
```

### We begin by randomly sampling 10,000 papers from the pdf_json folder. We can do this through the following linux command, utilizing the `shuf` command
`~  shuf -zn10000 -e document_parses/pdf_json/* | xargs -0 cp -vt random_sample/`

### Read the sample


```python
papers = spark.read\
            .format("json")\
            .option("multiLine", "true")\
            .load(home+"/Dataset/random_sample/")
```

## Step 2: Explore the data

### From the metadata exploration we found that many columns has nulls and some are only nulls


```python
metadata.printSchema()
```

    root
     |-- cord_uid: string (nullable = true)
     |-- sha: string (nullable = true)
     |-- source_x: string (nullable = true)
     |-- title: string (nullable = true)
     |-- doi: string (nullable = true)
     |-- pmcid: string (nullable = true)
     |-- pubmed_id: string (nullable = true)
     |-- license: string (nullable = true)
     |-- abstract: string (nullable = true)
     |-- publish_time: string (nullable = true)
     |-- authors: string (nullable = true)
     |-- journal: string (nullable = true)
     |-- mag_id: string (nullable = true)
     |-- who_covidence_id: string (nullable = true)
     |-- arxiv_id: string (nullable = true)
     |-- pdf_json_files: string (nullable = true)
     |-- pmc_json_files: string (nullable = true)
     |-- url: string (nullable = true)
     |-- s2_id: string (nullable = true)
    



```python
metadata.show(5)
```

    +--------+--------------------+--------+--------------------+--------------------+--------+---------+-------+--------------------+------------+--------------------+--------------+------+----------------+--------+--------------------+--------------------+--------------------+-----+
    |cord_uid|                 sha|source_x|               title|                 doi|   pmcid|pubmed_id|license|            abstract|publish_time|             authors|       journal|mag_id|who_covidence_id|arxiv_id|      pdf_json_files|      pmc_json_files|                 url|s2_id|
    +--------+--------------------+--------+--------------------+--------------------+--------+---------+-------+--------------------+------------+--------------------+--------------+------+----------------+--------+--------------------+--------------------+--------------------+-----+
    |ug7v899j|d1aafb70c066a2068...|     PMC|Clinical features...|10.1186/1471-2334...|PMC35282| 11472636|  no-cc|OBJECTIVE: This r...|  2001-07-04|Madani, Tariq A; ...|BMC Infect Dis|  null|            null|    null|document_parses/p...|document_parses/p...|https://www.ncbi....| null|
    |02tnwd4m|6b0567729c2143a66...|     PMC|Nitric oxide: a p...|        10.1186/rr14|PMC59543| 11667967|  no-cc|Inflammatory dise...|  2000-08-15|Vliet, Albert van...|    Respir Res|  null|            null|    null|document_parses/p...|document_parses/p...|https://www.ncbi....| null|
    |ejv2xln0|06ced00a5fc042159...|     PMC|Surfactant protei...|        10.1186/rr19|PMC59549| 11667972|  no-cc|Surfactant protei...|  2000-08-25|     Crouch, Erika C|    Respir Res|  null|            null|    null|document_parses/p...|document_parses/p...|https://www.ncbi....| null|
    |2b73a28n|348055649b6b8cf2b...|     PMC|Role of endotheli...|        10.1186/rr44|PMC59574| 11686871|  no-cc|Endothelin-1 (ET-...|  2001-02-22|Fagan, Karen A; M...|    Respir Res|  null|            null|    null|document_parses/p...|document_parses/p...|https://www.ncbi....| null|
    |9785vg6d|5f48792a5fa08bed9...|     PMC|Gene expression i...|        10.1186/rr61|PMC59580| 11686888|  no-cc|Respiratory syncy...|  2001-05-11|Domachowske, Jose...|    Respir Res|  null|            null|    null|document_parses/p...|document_parses/p...|https://www.ncbi....| null|
    +--------+--------------------+--------+--------------------+--------------------+--------+---------+-------+--------------------+------------+--------------------+--------------+------+----------------+--------+--------------------+--------------------+--------------------+-----+
    only showing top 5 rows
    



```python
from pyspark.sql.functions import isnan, when, count, col

metadata.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in metadata.columns]).show()
```

    +--------+-----+--------+-----+-----+-----+---------+-------+--------+------------+-------+-------+------+----------------+--------+--------------+--------------+-----+-----+
    |cord_uid|  sha|source_x|title|  doi|pmcid|pubmed_id|license|abstract|publish_time|authors|journal|mag_id|who_covidence_id|arxiv_id|pdf_json_files|pmc_json_files|  url|s2_id|
    +--------+-----+--------+-----+-----+-----+---------+-------+--------+------------+-------+-------+------+----------------+--------+--------------+--------------+-----+-----+
    |       0|77267|       0|   30|29459|71159|    32790|     41|   29300|          44|   4995|   6651|133300|          113503|  132417|         75636|         87526|11453|28314|
    +--------+-----+--------+-----+-----+-----+---------+-------+--------+------------+-------+-------+------+----------------+--------+--------------+--------------+-----+-----+
    


### The json files schema is available in the dataset where we found all the info we need


```python
papers.show(10)
```

    +--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
    |            abstract|         back_matter|         bib_entries|           body_text|            metadata|            paper_id|         ref_entries|
    +--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
    |                  []|[[[[48,, 44, 3.37...|[, [[[H, Adams, [...|[[[], [[79, TABRE...|[[[[,,], , , Bert...|2b1cbb43a4f06e232...|[[, Chest Wall Th...|
    |[[[], [], Abstrac...|[[[], [], acknowl...|[[[], , [,,,], , ...|[[[[1203,, 1200, ...|[[[[,,], , ;, Bey...|c9f4a4df803f6496b...|[[, Figure 2, fig...|
    |                  []|[[[[1609,, 1586, ...|[[[[T, Aaberg, [M...|[[[[505,, 497, Ka...|              [[], ]|5c6c26e79c0824645...|[,, [, Technische...|
    |                  []|[[[], [], annex, ...|[[[[Organizationa...|[[[], [], , LEARN...|[[[[University of...|38c0691ee76fb1e66...|[[, A 40 YEAR OLD...|
    |                  []|[[[], [], acknowl...|[[[], , [,,,], , ...|[[[[234,, 231, (1...|[[[[,,], , C, Dob...|67c8d389c28ceef26...|[[, (containing P...|
    |                  []|                  []|[[[], , [,,,], , ...|[[[[24,, 19, July...|              [[], ]|8c93a69ce8eb2ba08...|[[, . 2013;62:540...|
    |[[[], [], Abstrac...|[[[], [[10,, 1, (...|[[[], , [,,,], , ...|[[[], [], Univers...|[[[[Bloodworks NW...|e3b74d02ad582540a...|[[, BP7: RBC in-v...|
    |                  []|                  []|[[[[K, Aas, [], ]...|[[[], [], The Imm...|              [[], ]|7e62ae259b327bda2...|[[, Fig.1.2. (Con...|
    |                  []|[[[], [], acknowl...|[[[[V, Moura, [R]...|[[[[199,, 195, (P...|[[[[,,], , ,, Kra...|a78f60c67c8ac17c7...|[[, Thomas G. Bro...|
    |[[[], [], Abstrac...|[[[[808, BIBREF33...|[[[[A, Caudy, [A]...|[[[], [], Backgro...|[[[[Academy of Sc...|c60cc22e04f138a79...|[[, Argonaute pro...|
    +--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
    only showing top 10 rows
    


## Now we need to process the json files and get the info we need in a simple structured dataframe

### Define a function to gather all paragraphs in one text
### and gather abstract paragraphs and body paragraphs


```python
from pyspark.sql.functions import udf

def concatenate_text(j):
    txt_all = ""
    for a in j:
        txt_all = txt_all + " " + a['text']
    return txt_all

udf_concatenate_text = udf(concatenate_text)
```


```python
papers = papers.select(papers['Paper_ID'], papers['metadata']['title'], udf_concatenate_text(papers['abstract']), udf_concatenate_text(papers['body_text']))
```


```python
papers.printSchema()
```

    root
     |-- Paper_ID: string (nullable = true)
     |-- metadata.title: string (nullable = true)
     |-- concatenate_text(abstract): string (nullable = true)
     |-- concatenate_text(body_text): string (nullable = true)
    



```python
papers = papers.withColumnRenamed('metadata.title', 'title')\
        .withColumnRenamed('concatenate_text(abstract)', 'abstract')\
        .withColumnRenamed('concatenate_text(body_text)', 'body')
```


```python
papers.show(10)
```

    +--------------------+--------------------+--------------------+--------------------+
    |            Paper_ID|               title|            abstract|                body|
    +--------------------+--------------------+--------------------+--------------------+
    |2b1cbb43a4f06e232...|Level 3 guideline...|                    | S3-guideline on ...|
    |c9f4a4df803f6496b...|Physicians Poster...| Background: Prom...| with Thiotepa 5 ...|
    |5c6c26e79c0824645...|                    |                    | Der Weltgesundhe...|
    |38c0691ee76fb1e66...|Society of Genera...|                    | LEARNING OBJECTI...|
    |67c8d389c28ceef26...|        Diedrich (1)|                    | The presence of ...|
    |8c93a69ce8eb2ba08...|                    |                    | July-December 20...|
    |e3b74d02ad582540a...|P1-A03A IgG Subty...| Background/Case ...| Background/Case ...|
    |7e62ae259b327bda2...|                    |                    | Definitions. The...|
    |a78f60c67c8ac17c7...|Alterations in 14...|                    | Objective: Using...|
    |c60cc22e04f138a79...|EXTERNAL SCIENTIF...| This report is t...| This report is a...|
    +--------------------+--------------------+--------------------+--------------------+
    only showing top 10 rows
    


### Seems that some fields have empty values, let's explore that


```python
from pyspark.sql.functions import isnan, when, count, col

papers.select([count(when(col(c) == "", c)).alias(c) for c in papers.columns]).show()
```

    +--------+-----+--------+----+
    |Paper_ID|title|abstract|body|
    +--------+-----+--------+----+
    |       0| 1060|    2927|   0|
    +--------+-----+--------+----+
    


## Step 3: Prepare and process the data

### Now join the papers dataframe the metadata and get only the columns that we might use as features


```python
papers_meta = papers.join(metadata, papers['Paper_ID'] == metadata['sha'], how='left_outer')\
            .select(papers['Paper_ID'], papers['title'], papers['body'], metadata['publish_time'],\
                   metadata['authors'], metadata['journal'])
```


```python
from pyspark.sql.functions import year, month, to_date
papers_meta = papers_meta.withColumn("publish_Year", year(to_date("publish_time")))\
                        .withColumn("publish_Month", month(to_date("publish_time")))
```


```python
papers_meta.show(5)
```

    +--------------------+--------------------+--------------------+------------+--------------------+--------------------+------------+-------------+
    |            Paper_ID|               title|                body|publish_time|             authors|             journal|publish_Year|publish_Month|
    +--------------------+--------------------+--------------------+------------+--------------------+--------------------+------------+-------------+
    |0782b4fb23ca65baf...|The population ge...| The model will b...|  2015-11-30|Tibayrenc, Michel...|        Acta Tropica|        2015|           11|
    |14d8fd027f39c8311...|Drug-Induced Dela...| Drug-induced del...|  2015-05-15|Klimas, Natasha; ...|Cutaneous Drug Er...|        2015|            5|
    |1e5228e3f0658479a...|Canonicalizing Kn...| User-generated c...|  2020-04-17|Fatma, Nausheen; ...|Advances in Knowl...|        2020|            4|
    |1e663ac169e08ea02...|The effects of a ...| Evidence has bee...|  2006-07-31|Yip, Paul S.F.; F...|Journal of Affect...|        2006|            7|
    |1f26b5e8291ea1ddc...|The Case for Labo...| The field of pat...|  2017-07-16|Kaul, Karen L.; S...|         Acad Pathol|        2017|            7|
    +--------------------+--------------------+--------------------+------------+--------------------+--------------------+------------+-------------+
    only showing top 5 rows
    


### Define a function to detect the paper language and filter out non-english papers


```python
from langdetect import detect
def detect_lang(txt):
    try:
        return detect(txt)
    except:
        return None
udf_detect_lang = udf(detect_lang)
```


```python
papers_meta = papers_meta.withColumn('Lang', udf_detect_lang(papers_meta['body']))
```


```python
## some papers are indeed non English
papers_meta.select("*").where("Lang<>'en'").show(5)
```

    +--------------------+--------------------+--------------------+------------+--------------------+--------------------+------------+-------------+----+
    |            Paper_ID|               title|                body|publish_time|             authors|             journal|publish_Year|publish_Month|Lang|
    +--------------------+--------------------+--------------------+------------+--------------------+--------------------+------------+-------------+----+
    |8b99371978aab17bc...|                    | Pierre BÉGUÉ * A...|  2017-12-31|       Bégué, Pierre|Bulletin de l'Aca...|        2017|           12|  fr|
    |a97968e164a378d7d...|LE UN BOCAVIRUS H...| 6 -infection res...|  2006-11-30|Foulongne, Vincen...|Revue Francophone...|        2006|           11|  fr|
    |2806abcfcda76ad3e...|Journal Pre-proof...| grave de los cua...|  2020-04-29|Martino, Marcello...|                null|        2020|            4|  es|
    |8623787ac0fead8e9...|2 Infecties van d...| Luchtweginfectie...|        2011|de Jong, M.D.; Wo...|Microbiologie en ...|        2011|            1|  nl|
    |97a104cc4e0be18c3...|                    | Le terme de vasc...|  2012-12-21|           Dulac, Y.|Trait&#x000e9; de...|        2012|           12|  fr|
    +--------------------+--------------------+--------------------+------------+--------------------+--------------------+------------+-------------+----+
    only showing top 5 rows
    



```python
papers_meta.count()
```




    10017




```python
papers_meta = papers_meta.filter(papers_meta['Lang'] == 'en')
```


```python
papers_meta.count()
```




    9638




```python
#Drop the unneeded columns
papers_meta = papers_meta.drop('publish_time', 'Lang')
papers_meta.show(5)
```

    +--------------------+--------------------+--------------------+--------------------+--------------------+------------+-------------+
    |            Paper_ID|               title|                body|             authors|             journal|publish_Year|publish_Month|
    +--------------------+--------------------+--------------------+--------------------+--------------------+------------+-------------+
    |0782b4fb23ca65baf...|The population ge...| The model will b...|Tibayrenc, Michel...|        Acta Tropica|        2015|           11|
    |14d8fd027f39c8311...|Drug-Induced Dela...| Drug-induced del...|Klimas, Natasha; ...|Cutaneous Drug Er...|        2015|            5|
    |1e5228e3f0658479a...|Canonicalizing Kn...| User-generated c...|Fatma, Nausheen; ...|Advances in Knowl...|        2020|            4|
    |1e663ac169e08ea02...|The effects of a ...| Evidence has bee...|Yip, Paul S.F.; F...|Journal of Affect...|        2006|            7|
    |1f26b5e8291ea1ddc...|The Case for Labo...| The field of pat...|Kaul, Karen L.; S...|         Acad Pathol|        2017|            7|
    +--------------------+--------------------+--------------------+--------------------+--------------------+------------+-------------+
    only showing top 5 rows
    


### We will periodically save the dataframes we are working on in parquet format with 12 partitions (the number of logical cores in our machine) for performance improvements.


```python
## Save what we have so far
papers_meta.repartition(12).write.parquet("./Dataset/papers_meta")
```


```python
#papers_meta = spark.read\
#            .format("parquet")\
#            .option("header", "true")\
#            .load("./Dataset/papers_meta")
```

### Find which columns contain null or unknown values and replace them with " " for categorical and 0 for numerical

### Count Nulls and Empty values in each column to understand more about the data


```python
from pyspark.sql.functions import isnan, when, count, col
print("Count of Null")
papers_meta.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in papers_meta.columns]).show()
print("Count of Empty values")
papers_meta.select([count(when(col(c) == "", c)).alias(c) for c in papers_meta.columns]).show()
```

    Count of Null
    +--------+-----+----+-------+-------+------------+-------------+
    |Paper_ID|title|body|authors|journal|publish_Year|publish_Month|
    +--------+-----+----+-------+-------+------------+-------------+
    |       0|    0|   0|   1252|   1817|        1127|         1127|
    +--------+-----+----+-------+-------+------------+-------------+
    
    Count of Empty values
    +--------+-----+----+-------+-------+------------+-------------+
    |Paper_ID|title|body|authors|journal|publish_Year|publish_Month|
    +--------+-----+----+-------+-------+------------+-------------+
    |       0|  933|   0|      0|      0|           0|            0|
    +--------+-----+----+-------+-------+------------+-------------+
    



```python
cat_cols = [item[0] for item in papers_meta.dtypes if item[1].startswith('string')] 
cat_cols
```




    ['Paper_ID', 'title', 'body', 'authors', 'journal']




```python
from pyspark.sql.functions import col

cat_null_cols = [column for column in cat_cols if papers_meta.where(col(column).isNull()| col(column).isin('')).count() > 0]
cat_null_cols
```




    ['title', 'authors', 'journal']




```python
### Now let's fill with " "
for column in cat_null_cols:
    papers_meta = papers_meta.na.fill(" ")
```


```python
num_cols = [item[0] for item in papers_meta.dtypes if item[1].startswith('int') | item[1].startswith('double')] 
num_cols
```




    ['publish_Year', 'publish_Month']




```python
### Now let's find numerical columns with null values
num_null_cols = [column for column in num_cols if papers_meta.filter(col(column).isNull() | col(column).eqNullSafe(0)).count() > 0]
num_null_cols
```




    ['publish_Year', 'publish_Month']




```python
### Now let's fill with 0
for column in num_null_cols:
    papers_meta = papers_meta.na.fill(0)
```


```python
papers_meta.show(10)
```

    +--------------------+--------------------+--------------------+--------------------+--------------------+------------+-------------+
    |            Paper_ID|               title|                body|             authors|             journal|publish_Year|publish_Month|
    +--------------------+--------------------+--------------------+--------------------+--------------------+------------+-------------+
    |012b16ae54779ca1a...|Synchronized Bive...| Miniaturized imp...|Lyu, Hongming; Jo...|             Sci Rep|        2020|            2|
    |01bc7fe59fc7feb0e...|Open Forum Infect...| Acute upper resp...|Joseph, Patrick; ...|Open Forum Infect...|        2018|            2|
    |0240a12c9fdf6c031...|Supersize me: how...| In epidemiology,...|Kao, Rowland R.; ...|Trends in Microbi...|        2014|            5|
    |03bfb747583f6b214...|The Infant Nasoph...| The human microb...|                    |                    |           0|            0|
    |06e9041ff3cb1db28...|Selected Nonvacci...| A cute respirato...|Lee, Terrence; Jo...|American Journal ...|        2005|            4|
    |0760e79585cd85c7e...|                    | Infectious disea...|           Froes, F.|         Pulmonology|        2020|            4|
    |0adf2e22eefb4ea5e...|Aberrant coagulat...| Influenza A viru...|Yang, Yan; Tang, ...|Cellular & Molecu...|        2016|            4|
    |0b137fde2327ae466...|Tips and Tricks f...| Manufacturing of...|Viganò, Mariele; ...|      Stem Cells Int|        2018|            9|
    |0c95f3083af8f4daf...|Automated TruTip ...| Nucleic acid tec...|Thakore, Nitu; No...|            PLoS One|        2018|            7|
    |0ef55241b6127c6bc...|Putative papain-r...| Polyprotein proc...|                    |                    |           0|            0|
    +--------------------+--------------------+--------------------+--------------------+--------------------+------------+-------------+
    only showing top 10 rows
    


### Now drop duplicates rows (No duplicates found in our sample)


```python
papers_meta.count()
papers_meta.dropDuplicates()
```




    DataFrame[Paper_ID: string, title: string, body: string, authors: string, journal: string, publish_Year: int, publish_Month: int]




```python
papers_meta.count()
```




    9639




```python
## Save what we have so far
papers_meta.repartition(12).write.parquet("./Dataset/papers_meta_cleaned")
```


```python
papers_meta = spark.read\
            .format("parquet")\
            .option("header", "true")\
            .load("./Dataset/papers_meta_cleaned")
```

## Step 4 (Preprocessing)

### Now let's start the Processing phase for the papers body and title


```python
from pyspark.sql.functions import lower, regexp_replace
from pyspark.ml.feature import Tokenizer, StopWordsRemover
```

#### Convert the body text to lower case
#### Remove Punctuation


```python
from pyspark.sql.functions import col

papers_meta = papers_meta.withColumn("body", lower(col('body')))
papers_meta = papers_meta.withColumn("body", regexp_replace("body", "[^a-zA-Z\\s]" , " "))
papers_meta = papers_meta.withColumn("body", regexp_replace("body", " +" , " "))
papers_meta = papers_meta.withColumn("body", regexp_replace("body", "^ +" , ""))
```


```python
papers_meta = papers_meta.withColumn("title", lower(col('title')))
papers_meta = papers_meta.withColumn("title", regexp_replace("title", "[^a-zA-Z\\s]" , " "))
papers_meta = papers_meta.withColumn("title", regexp_replace("title", " +" , " "))
papers_meta = papers_meta.withColumn("title", regexp_replace("title", "^ +" , ""))
```


```python
papers_meta.show(10)
```

    +--------------------+--------------------+--------------------+--------------------+------------+------------+-------------+
    |            Paper_ID|               title|                body|             authors|     journal|publish_Year|publish_Month|
    +--------------------+--------------------+--------------------+--------------------+------------+------------+-------------+
    |002f213aeda7ce843...|etiology and impa...|community acquire...|Nolan, Vikki G; A...|J Infect Dis|        2018|            7|
    |0297dd12949520da3...|optimization of p...|the localization ...|Quintana, C.; Mar...|      Micron|        1998|            8|
    |09045f5964f24691f...|evaluation of tnf...|the toxic effects...|Rook, Graham A. W...|  Biotherapy|        1991|            1|
    |0e3da58a0d46d88ee...|the influence of ...|increasing our un...|                    |            |           0|            0|
    |103c89c60d7d24bb8...|does pathogen spi...|pathogen outbreak...|Otterstatter, Mic...|    PLoS One|        2008|            7|
    |12d9952ba3cf8410e...|immunoglobulin he...|immunoglobulin ge...|                    |            |           0|            0|
    |18f991e0b4dc59943...|center for medica...|the novel coronav...|Huang, Ying hui; ...|            |        2020|            4|
    |192b2c4501ca09903...|generalized latti...|several graph rep...|González-Díaz, H....|J Theor Biol|        2009|           11|
    |1a5c7512b0e842a7b...|sars coronavirus ...|severe acute resp...|Varshney, Bhavna;...|    PLoS One|        2012|            1|
    |1ba8aa57522bdeb6b...|genetic cellular ...|the year occasion...|       Nabel, Gary J|     Nat Med|        2004|            1|
    +--------------------+--------------------+--------------------+--------------------+------------+------------+-------------+
    only showing top 10 rows
    


#### Tokenize the paper body text and title text


```python
tokenizer = Tokenizer(inputCol='body', outputCol='words_token')
papers_meta = tokenizer.transform(papers_meta).select('*')
```


```python
tokenizer = Tokenizer(inputCol='title', outputCol='title_token')
papers_meta = tokenizer.transform(papers_meta).select('*')
```


```python
papers_meta.select('words_token', 'title_token').show(10)
```

    +--------------------+--------------------+
    |         words_token|         title_token|
    +--------------------+--------------------+
    |[community, acqui...|[etiology, and, i...|
    |[the, localizatio...|[optimization, of...|
    |[the, toxic, effe...|[evaluation, of, ...|
    |[increasing, our,...|[the, influence, ...|
    |[pathogen, outbre...|[does, pathogen, ...|
    |[immunoglobulin, ...|[immunoglobulin, ...|
    |[the, novel, coro...|[center, for, med...|
    |[several, graph, ...|[generalized, lat...|
    |[severe, acute, r...|[sars, coronaviru...|
    |[the, year, occas...|[genetic, cellula...|
    +--------------------+--------------------+
    only showing top 10 rows
    


#### Remove stop words


```python
remover = StopWordsRemover(inputCol='words_token', outputCol='words_clean')
papers_meta = remover.transform(papers_meta).select('*')
```


```python
remover = StopWordsRemover(inputCol='title_token', outputCol='title_clean')
papers_meta = remover.transform(papers_meta).select('*')
```


```python
papers_meta.select('words_clean', 'title_clean').show(10)
```

    +--------------------+--------------------+
    |         words_clean|         title_clean|
    +--------------------+--------------------+
    |[community, acqui...|[etiology, impact...|
    |[localization, ch...|[optimization, ph...|
    |[toxic, effects, ...|[evaluation, tnf,...|
    |[increasing, unde...|[influence, clima...|
    |[pathogen, outbre...|[pathogen, spillo...|
    |[immunoglobulin, ...|[immunoglobulin, ...|
    |[novel, coronavir...|[center, medical,...|
    |[several, graph, ...|[generalized, lat...|
    |[severe, acute, r...|[sars, coronaviru...|
    |[year, occasioned...|[genetic, cellula...|
    +--------------------+--------------------+
    only showing top 10 rows
    


#### Now we need to remove the custom stopwords


```python
remover2 = StopWordsRemover(inputCol='words_clean', outputCol='words_clean_custom', stopWords = ['doi', 'preprint', 'copyright', 'peer', 'reviewed', 'org',
'https', 'et', 'al', 'author', 'figure','rights', 'reserved', 'permission', 'used', 'using',
'biorxiv', 'medrxiv', 'license', 'fig', 'fig.', 'al.', 'Elsevier', 'PMC', 'CZI', 'www'])
papers_meta = remover2.transform(papers_meta).select('*')
```


```python
    remover2 = StopWordsRemover(inputCol='title_clean', outputCol='title_clean_custom', stopWords = ['doi', 'preprint', 'copyright', 'peer', 'reviewed', 'org',
'https', 'et', 'al', 'author', 'figure','rights', 'reserved', 'permission', 'used', 'using',
'biorxiv', 'medrxiv', 'license', 'fig', 'fig.', 'al.', 'Elsevier', 'PMC', 'CZI', 'www'])
papers_meta = remover2.transform(papers_meta).select('*')
```


```python
papers_meta.select('words_clean_custom', 'title_clean_custom').show(10)
```

    +--------------------+--------------------+
    |  words_clean_custom|  title_clean_custom|
    +--------------------+--------------------+
    |[community, acqui...|[etiology, impact...|
    |[localization, ch...|[optimization, ph...|
    |[toxic, effects, ...|[evaluation, tnf,...|
    |[increasing, unde...|[influence, clima...|
    |[pathogen, outbre...|[pathogen, spillo...|
    |[immunoglobulin, ...|[immunoglobulin, ...|
    |[novel, coronavir...|[center, medical,...|
    |[several, graph, ...|[generalized, lat...|
    |[severe, acute, r...|[sars, coronaviru...|
    |[year, occasioned...|[genetic, cellula...|
    +--------------------+--------------------+
    only showing top 10 rows
    



```python
from pyspark.sql.functions import size
papers_meta = papers_meta.withColumn("wordcount", size("words_clean_custom"))
```


```python
papers_meta.printSchema()
```

    root
     |-- Paper_ID: string (nullable = true)
     |-- title: string (nullable = true)
     |-- body: string (nullable = true)
     |-- authors: string (nullable = true)
     |-- journal: string (nullable = true)
     |-- publish_Year: integer (nullable = true)
     |-- publish_Month: integer (nullable = true)
     |-- words_token: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- title_token: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- words_clean: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- title_clean: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- words_clean_custom: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- title_clean_custom: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- wordcount: integer (nullable = false)
    


### Drop the unneeded columns


```python
#papers_meta = papers_meta.drop('title', 'body', 'words_token', 'words_clean', 'title_token', 'title_clean')
papers_meta = papers_meta.drop('body', 'words_token', 'words_clean', 'title_token')
```


```python
papers_meta.show()
```

    +--------------------+--------------------+--------------------+--------------------+------------+-------------+--------------------+--------------------+--------------------+---------+
    |            Paper_ID|               title|             authors|             journal|publish_Year|publish_Month|         title_clean|  words_clean_custom|  title_clean_custom|wordcount|
    +--------------------+--------------------+--------------------+--------------------+------------+-------------+--------------------+--------------------+--------------------+---------+
    |002f213aeda7ce843...|etiology and impa...|Nolan, Vikki G; A...|        J Infect Dis|        2018|            7|[etiology, impact...|[community, acqui...|[etiology, impact...|     2116|
    |0297dd12949520da3...|optimization of p...|Quintana, C.; Mar...|              Micron|        1998|            8|[optimization, ph...|[localization, ch...|[optimization, ph...|     2837|
    |09045f5964f24691f...|evaluation of tnf...|Rook, Graham A. W...|          Biotherapy|        1991|            1|[evaluation, tnf,...|[toxic, effects, ...|[evaluation, tnf,...|     1834|
    |0e3da58a0d46d88ee...|the influence of ...|                    |                    |           0|            0|[influence, clima...|[increasing, unde...|[influence, clima...|     2549|
    |103c89c60d7d24bb8...|does pathogen spi...|Otterstatter, Mic...|            PLoS One|        2008|            7|[pathogen, spillo...|[pathogen, outbre...|[pathogen, spillo...|     3507|
    |12d9952ba3cf8410e...|immunoglobulin he...|                    |                    |           0|            0|[immunoglobulin, ...|[immunoglobulin, ...|[immunoglobulin, ...|     3212|
    |18f991e0b4dc59943...|center for medica...|Huang, Ying hui; ...|                    |        2020|            4|[center, medical,...|[novel, coronavir...|[center, medical,...|     1846|
    |192b2c4501ca09903...|generalized latti...|González-Díaz, H....|        J Theor Biol|        2009|           11|[generalized, lat...|[several, graph, ...|[generalized, lat...|     4797|
    |1a5c7512b0e842a7b...|sars coronavirus ...|Varshney, Bhavna;...|            PLoS One|        2012|            1|[sars, coronaviru...|[severe, acute, r...|[sars, coronaviru...|     2911|
    |1ba8aa57522bdeb6b...|genetic cellular ...|       Nabel, Gary J|             Nat Med|        2004|            1|[genetic, cellula...|[year, occasioned...|[genetic, cellula...|     2507|
    |37c23803454ff0985...|                    |Ruuskanen, Olli; ...|     Clin Infect Dis|        2014|            6|                  []|[editor, rhinovir...|                  []|      727|
    |3dcaec279aea60700...|protection agains...|                    |                    |           0|            0|[protection, filo...|[filoviruses, eme...|[protection, filo...|     1489|
    |52da582d52fe94db1...|apoptosis induced...|Goswami, Biswendu...|       Antiviral Res|        2004|            4|[apoptosis, induc...|[hepatitis, virus...|[apoptosis, induc...|     3550|
    |5b01c0f218d069ed3...|rapid quantitativ...|Yan, Zhongqiang; ...|Sens Actuators B ...|        2006|           12|[rapid, quantitat...|[yersinia, pestis...|[rapid, quantitat...|     2171|
    |5fcb93b865d4cd747...|vitamin b and vit...|Song, Ningning; Z...|     Front Microbiol|        2020|            4|[vitamin, b, vita...|[mycobacterium, t...|[vitamin, b, vita...|     3603|
    |6475715fd9bca1168...|light dark photon...|Nakai, Yuichiro; ...|                    |        2020|            4|[light, dark, pho...|[nature, dark, ma...|[light, dark, pho...|     2809|
    |6ac37ecc83bd4e2f6...|article molecular...|Pickett, Julie E....|            Bone Res|        2018|            4|[article, molecul...|[total, joint, ar...|[article, molecul...|     2945|
    |6cf3e75deed023c8e...|chapter efficient...|Cockrell, Adam S....|Reverse Genetics ...|        2017|            5|[chapter, efficie...|[human, coronavir...|[chapter, efficie...|     2203|
    |6d0f164901ae18557...|autoantibodies in...|Biedzka‐Sarek, M....|     Scand J Immunol|        2008|            6|[autoantibodies, ...|[nuclear, recepto...|[autoantibodies, ...|     9375|
    |740e2079b3b8d3391...|emerging technolo...|White, David M.; ...|Ensuring National...|        2016|           12|[emerging, techno...|[role, institutio...|[emerging, techno...|     4597|
    +--------------------+--------------------+--------------------+--------------------+------------+-------------+--------------------+--------------------+--------------------+---------+
    only showing top 20 rows
    



```python
papers_meta.repartition(12).write.parquet("./papers_meta_processed")
```


```python
#papers_meta = spark.read\
#            .format("parquet")\
#            .option("header", "true")\
#            .load("./Dataset/papers_meta_processed")
```

## Step 5 (Vectorization and prepare the features column)

#### Now Apply Word2Vec on the processed body text "words_clean_custom" and the title text "title_clean_custom"


```python
from pyspark.ml.feature import Word2Vec

# Learn a mapping from words to Vectors.
word2Vec = Word2Vec(vectorSize=100, minCount=0, inputCol="words_clean_custom", outputCol="word2vec_body")
model = word2Vec.fit(papers_meta)

papers_meta = model.transform(papers_meta)
```


```python
word2Vec = Word2Vec(vectorSize=100, minCount=0, inputCol="title_clean_custom", outputCol="word2vec_title")
model = word2Vec.fit(papers_meta)

papers_meta = model.transform(papers_meta)
```


```python
papers_meta.show(10)
```

    +--------------------+--------------------+--------------------+------------+------------+-------------+--------------------+--------------------+--------------------+---------+--------------------+--------------------+
    |            Paper_ID|               title|             authors|     journal|publish_Year|publish_Month|         title_clean|  words_clean_custom|  title_clean_custom|wordcount|       word2vec_body|      word2vec_title|
    +--------------------+--------------------+--------------------+------------+------------+-------------+--------------------+--------------------+--------------------+---------+--------------------+--------------------+
    |002f213aeda7ce843...|etiology and impa...|Nolan, Vikki G; A...|J Infect Dis|        2018|            7|[etiology, impact...|[community, acqui...|[etiology, impact...|     2116|[0.06443376156094...|[0.00269449857296...|
    |0297dd12949520da3...|optimization of p...|Quintana, C.; Mar...|      Micron|        1998|            8|[optimization, ph...|[localization, ch...|[optimization, ph...|     2837|[0.10042087261533...|[-0.0050401708867...|
    |09045f5964f24691f...|evaluation of tnf...|Rook, Graham A. W...|  Biotherapy|        1991|            1|[evaluation, tnf,...|[toxic, effects, ...|[evaluation, tnf,...|     1834|[0.04337179083320...|[-0.0120733820755...|
    |0e3da58a0d46d88ee...|the influence of ...|                    |            |           0|            0|[influence, clima...|[increasing, unde...|[influence, clima...|     2549|[0.10467938052221...|[-0.0949713807785...|
    |103c89c60d7d24bb8...|does pathogen spi...|Otterstatter, Mic...|    PLoS One|        2008|            7|[pathogen, spillo...|[pathogen, outbre...|[pathogen, spillo...|     3507|[0.05590694534083...|[6.79323734301659...|
    |12d9952ba3cf8410e...|immunoglobulin he...|                    |            |           0|            0|[immunoglobulin, ...|[immunoglobulin, ...|[immunoglobulin, ...|     3212|[0.02539976464512...|[-0.0145119832640...|
    |18f991e0b4dc59943...|center for medica...|Huang, Ying hui; ...|            |        2020|            4|[center, medical,...|[novel, coronavir...|[center, medical,...|     1846|[0.02266752335855...|[0.04260698920633...|
    |192b2c4501ca09903...|generalized latti...|González-Díaz, H....|J Theor Biol|        2009|           11|[generalized, lat...|[several, graph, ...|[generalized, lat...|     4797|[0.09675638989127...|[-0.0113056179002...|
    |1a5c7512b0e842a7b...|sars coronavirus ...|Varshney, Bhavna;...|    PLoS One|        2012|            1|[sars, coronaviru...|[severe, acute, r...|[sars, coronaviru...|     2911|[0.05548735486484...|[-0.0214845975395...|
    |1ba8aa57522bdeb6b...|genetic cellular ...|       Nabel, Gary J|     Nat Med|        2004|            1|[genetic, cellula...|[year, occasioned...|[genetic, cellula...|     2507|[0.04130894332898...|[0.00482482530060...|
    +--------------------+--------------------+--------------------+------------+------------+-------------+--------------------+--------------------+--------------------+---------+--------------------+--------------------+
    only showing top 10 rows
    



```python
papers_meta.repartition(12).write.parquet("./Dataset/papers_meta_word2vec")
```


```python
papers_meta = spark.read\
            .format("parquet")\
            .option("header", "true")\
            .load("./Dataset/papers_meta_word2vec")
```


```python
papers_meta.printSchema()
```

    root
     |-- Paper_ID: string (nullable = true)
     |-- title: string (nullable = true)
     |-- authors: string (nullable = true)
     |-- journal: string (nullable = true)
     |-- publish_Year: integer (nullable = true)
     |-- publish_Month: integer (nullable = true)
     |-- title_clean: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- words_clean_custom: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- title_clean_custom: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- wordcount: integer (nullable = true)
     |-- word2vec_body: vector (nullable = true)
     |-- word2vec_title: vector (nullable = true)
    



```python
#Select only the needed columns
papers_meta = papers_meta.select('Paper_ID', 'authors', 'journal', 'wordcount', 'publish_Year', 'publish_Month', 'word2vec_body', 'word2vec_title')
```


```python
papers_meta.show(5)
```

    +--------------------+--------------------+--------------------+---------+------------+-------------+--------------------+--------------------+
    |            Paper_ID|             authors|             journal|wordcount|publish_Year|publish_Month|       word2vec_body|      word2vec_title|
    +--------------------+--------------------+--------------------+---------+------------+-------------+--------------------+--------------------+
    |012b16ae54779ca1a...|Lyu, Hongming; Jo...|             Sci Rep|     1698|        2020|            2|[0.01163073299174...|[-0.0075389318427...|
    |01bc7fe59fc7feb0e...|Joseph, Patrick; ...|Open Forum Infect...|     2106|        2018|            2|[0.08144156184301...|[-0.0090094477359...|
    |0240a12c9fdf6c031...|Kao, Rowland R.; ...|Trends in Microbi...|     3808|        2014|            5|[0.04915223334121...|[-0.0112262216571...|
    |03bfb747583f6b214...|                    |                    |     3848|           0|            0|[0.03949146437089...|[0.00115656061097...|
    |06e9041ff3cb1db28...|Lee, Terrence; Jo...|American Journal ...|     2619|        2005|            4|[0.09074272874731...|[-0.0065258390604...|
    +--------------------+--------------------+--------------------+---------+------------+-------------+--------------------+--------------------+
    only showing top 5 rows
    


### Now let's prepare the features vector

#### First, Define StringIndexers for categorical columns


```python
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
```


```python
cat_cols = [item[0] for item in papers_meta.dtypes if item[1].startswith('string')][1:]
cat_cols
```




    ['authors', 'journal']




```python
indexers = [StringIndexer(
    inputCol=column, 
    outputCol=column + '_index', 
    handleInvalid='keep') for column in cat_cols]
```


```python
encoders = [OneHotEncoder(
    inputCol=column + '_index', 
    outputCol= column + '_encoded') for column in cat_cols]
```


```python
from pyspark.ml import Pipeline

pipeline = Pipeline(stages=indexers + encoders)
```


```python
papers_meta_transformed = pipeline.fit(papers_meta).transform(papers_meta)
papers_meta_transformed.show(10)
```

    +--------------------+--------------------+--------------------+---------+------------+-------------+--------------------+--------------------+-------------+-------------+-------------------+-------------------+
    |            Paper_ID|             authors|             journal|wordcount|publish_Year|publish_Month|       word2vec_body|      word2vec_title|authors_index|journal_index|    authors_encoded|    journal_encoded|
    +--------------------+--------------------+--------------------+---------+------------+-------------+--------------------+--------------------+-------------+-------------+-------------------+-------------------+
    |012b16ae54779ca1a...|Lyu, Hongming; Jo...|             Sci Rep|     1698|        2020|            2|[0.01163073299174...|[-0.0075389318427...|       7681.0|          7.0|(8321,[7681],[1.0])|   (2750,[7],[1.0])|
    |01bc7fe59fc7feb0e...|Joseph, Patrick; ...|Open Forum Infect...|     2106|        2018|            2|[0.08144156184301...|[-0.0090094477359...|       1436.0|         92.0|(8321,[1436],[1.0])|  (2750,[92],[1.0])|
    |0240a12c9fdf6c031...|Kao, Rowland R.; ...|Trends in Microbi...|     3808|        2014|            5|[0.04915223334121...|[-0.0112262216571...|       3535.0|        704.0|(8321,[3535],[1.0])| (2750,[704],[1.0])|
    |03bfb747583f6b214...|                    |                    |     3848|           0|            0|[0.03949146437089...|[0.00115656061097...|          0.0|          0.0|   (8321,[0],[1.0])|   (2750,[0],[1.0])|
    |06e9041ff3cb1db28...|Lee, Terrence; Jo...|American Journal ...|     2619|        2005|            4|[0.09074272874731...|[-0.0065258390604...|       3552.0|        357.0|(8321,[3552],[1.0])| (2750,[357],[1.0])|
    |0760e79585cd85c7e...|           Froes, F.|         Pulmonology|      477|        2020|            4|[0.07848690268100...|[0.00330294785089...|       7879.0|       1169.0|(8321,[7879],[1.0])|(2750,[1169],[1.0])|
    |0adf2e22eefb4ea5e...|Yang, Yan; Tang, ...|Cellular & Molecu...|     3184|        2016|            4|[-0.0165081877109...|[-0.0174122095664...|       8314.0|        398.0|(8321,[8314],[1.0])| (2750,[398],[1.0])|
    |0b137fde2327ae466...|Viganò, Mariele; ...|      Stem Cells Int|     4208|        2018|            9|[0.05817865815495...|[-0.0057283864339...|       6888.0|       2276.0|(8321,[6888],[1.0])|(2750,[2276],[1.0])|
    |0c95f3083af8f4daf...|Thakore, Nitu; No...|            PLoS One|     1980|        2018|            7|[0.05883210357652...|[0.00249506733962...|        704.0|          1.0| (8321,[704],[1.0])|   (2750,[1],[1.0])|
    |0ef55241b6127c6bc...|                    |                    |      941|           0|            0|[0.06429963556128...|[-0.0121964396426...|          0.0|          0.0|   (8321,[0],[1.0])|   (2750,[0],[1.0])|
    +--------------------+--------------------+--------------------+---------+------------+-------------+--------------------+--------------------+-------------+-------------+-------------------+-------------------+
    only showing top 10 rows
    


### Now select the required features and apply the vector assembler


```python
requiredFeatures = [
    'wordcount',
    'publish_Year',
    'publish_Month',
    'word2vec_body',
    'word2vec_title',
    'authors_encoded',
    'journal_encoded'
]
```


```python
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(inputCols=requiredFeatures, outputCol='features')
```


```python
papers_meta_transformed = assembler.transform(papers_meta_transformed)
papers_meta_transformed.show(10)
```

    +--------------------+--------------------+--------------------+---------+------------+-------------+--------------------+--------------------+-------------+-------------+-------------------+-------------------+--------------------+
    |            Paper_ID|             authors|             journal|wordcount|publish_Year|publish_Month|       word2vec_body|      word2vec_title|authors_index|journal_index|    authors_encoded|    journal_encoded|            features|
    +--------------------+--------------------+--------------------+---------+------------+-------------+--------------------+--------------------+-------------+-------------+-------------------+-------------------+--------------------+
    |012b16ae54779ca1a...|Lyu, Hongming; Jo...|             Sci Rep|     1698|        2020|            2|[0.01163073299174...|[-0.0075389318427...|       7681.0|          7.0|(8321,[7681],[1.0])|   (2750,[7],[1.0])|(11274,[0,1,2,3,4...|
    |01bc7fe59fc7feb0e...|Joseph, Patrick; ...|Open Forum Infect...|     2106|        2018|            2|[0.08144156184301...|[-0.0090094477359...|       1436.0|         92.0|(8321,[1436],[1.0])|  (2750,[92],[1.0])|(11274,[0,1,2,3,4...|
    |0240a12c9fdf6c031...|Kao, Rowland R.; ...|Trends in Microbi...|     3808|        2014|            5|[0.04915223334121...|[-0.0112262216571...|       3535.0|        704.0|(8321,[3535],[1.0])| (2750,[704],[1.0])|(11274,[0,1,2,3,4...|
    |03bfb747583f6b214...|                    |                    |     3848|           0|            0|[0.03949146437089...|[0.00115656061097...|          0.0|          0.0|   (8321,[0],[1.0])|   (2750,[0],[1.0])|(11274,[0,3,4,5,6...|
    |06e9041ff3cb1db28...|Lee, Terrence; Jo...|American Journal ...|     2619|        2005|            4|[0.09074272874731...|[-0.0065258390604...|       3552.0|        357.0|(8321,[3552],[1.0])| (2750,[357],[1.0])|(11274,[0,1,2,3,4...|
    |0760e79585cd85c7e...|           Froes, F.|         Pulmonology|      477|        2020|            4|[0.07848690268100...|[0.00330294785089...|       7879.0|       1169.0|(8321,[7879],[1.0])|(2750,[1169],[1.0])|(11274,[0,1,2,3,4...|
    |0adf2e22eefb4ea5e...|Yang, Yan; Tang, ...|Cellular & Molecu...|     3184|        2016|            4|[-0.0165081877109...|[-0.0174122095664...|       8314.0|        398.0|(8321,[8314],[1.0])| (2750,[398],[1.0])|(11274,[0,1,2,3,4...|
    |0b137fde2327ae466...|Viganò, Mariele; ...|      Stem Cells Int|     4208|        2018|            9|[0.05817865815495...|[-0.0057283864339...|       6888.0|       2276.0|(8321,[6888],[1.0])|(2750,[2276],[1.0])|(11274,[0,1,2,3,4...|
    |0c95f3083af8f4daf...|Thakore, Nitu; No...|            PLoS One|     1980|        2018|            7|[0.05883210357652...|[0.00249506733962...|        704.0|          1.0| (8321,[704],[1.0])|   (2750,[1],[1.0])|(11274,[0,1,2,3,4...|
    |0ef55241b6127c6bc...|                    |                    |      941|           0|            0|[0.06429963556128...|[-0.0121964396426...|          0.0|          0.0|   (8321,[0],[1.0])|   (2750,[0],[1.0])|(11274,[0,3,4,5,6...|
    +--------------------+--------------------+--------------------+---------+------------+-------------+--------------------+--------------------+-------------+-------------+-------------------+-------------------+--------------------+
    only showing top 10 rows
    



```python
papers_meta_transformed.select('features').head(1)
```




    [Row(features=SparseVector(11274, {0: 1698.0, 1: 2020.0, 2: 2.0, 3: 0.0116, 4: 0.0265, 5: -0.0644, 6: 0.0816, 7: 0.0961, 8: 0.0552, 9: -0.0292, 10: -0.0011, 11: 0.0366, 12: -0.023, 13: 0.0719, 14: 0.0557, 15: 0.0626, 16: -0.0705, 17: 0.0417, 18: -0.0005, 19: 0.0572, 20: 0.0365, 21: -0.0463, 22: -0.0642, 23: 0.0766, 24: 0.1025, 25: -0.0562, 26: -0.0541, 27: 0.1363, 28: 0.0014, 29: 0.0375, 30: -0.01, 31: 0.0259, 32: -0.0201, 33: 0.0728, 34: -0.0501, 35: 0.0868, 36: 0.0254, 37: 0.0173, 38: 0.0262, 39: 0.049, 40: 0.0031, 41: -0.0995, 42: 0.024, 43: 0.057, 44: -0.1118, 45: 0.0655, 46: -0.0249, 47: -0.0041, 48: 0.1353, 49: 0.0664, 50: -0.0821, 51: 0.0134, 52: 0.1031, 53: 0.0146, 54: 0.0308, 55: -0.0068, 56: -0.0535, 57: 0.0218, 58: -0.0081, 59: -0.091, 60: 0.0346, 61: 0.0121, 62: 0.031, 63: -0.0908, 64: -0.0987, 65: -0.0094, 66: 0.049, 67: 0.0227, 68: -0.0361, 69: -0.0589, 70: 0.0279, 71: 0.0328, 72: 0.0705, 73: 0.027, 74: -0.0416, 75: 0.0925, 76: 0.1131, 77: 0.0295, 78: 0.0567, 79: -0.0371, 80: -0.054, 81: -0.014, 82: 0.0098, 83: 0.0622, 84: 0.0198, 85: 0.0181, 86: 0.0543, 87: 0.0255, 88: -0.1185, 89: -0.0375, 90: 0.0758, 91: 0.0095, 92: 0.0429, 93: 0.0596, 94: 0.0391, 95: -0.0748, 96: 0.0263, 97: 0.0678, 98: -0.0216, 99: -0.0596, 100: -0.0636, 101: 0.0, 102: 0.0382, 103: -0.0075, 104: 0.0001, 105: -0.0093, 106: 0.0027, 107: 0.0065, 108: -0.001, 109: 0.013, 110: -0.0179, 111: 0.0049, 112: -0.0194, 113: -0.0021, 114: 0.0033, 115: -0.0164, 116: -0.0087, 117: 0.0145, 118: -0.0014, 119: 0.0244, 120: -0.0, 121: 0.0129, 122: -0.0324, 123: -0.0123, 124: -0.0077, 125: -0.0043, 126: 0.0036, 127: 0.0001, 128: -0.0139, 129: -0.0042, 130: 0.0041, 131: 0.0164, 132: 0.0062, 133: -0.0053, 134: 0.0025, 135: -0.0028, 136: -0.0011, 137: -0.0028, 138: -0.0032, 139: 0.0163, 140: 0.0121, 141: 0.0224, 142: -0.002, 143: 0.0127, 144: -0.0115, 145: 0.0105, 146: 0.0016, 147: 0.0037, 148: 0.0058, 149: 0.0087, 150: -0.0036, 151: 0.0137, 152: 0.0089, 153: -0.0131, 154: 0.0114, 155: -0.0016, 156: 0.004, 157: -0.0019, 158: -0.0083, 159: -0.0138, 160: -0.0043, 161: 0.0195, 162: 0.0058, 163: 0.0204, 164: -0.0064, 165: 0.0114, 166: -0.0042, 167: 0.0156, 168: -0.0144, 169: -0.0014, 170: 0.011, 171: 0.0032, 172: 0.0087, 173: 0.0047, 174: -0.0181, 175: 0.0036, 176: 0.0118, 177: -0.0159, 178: -0.0097, 179: 0.0049, 180: 0.0026, 181: 0.0163, 182: -0.0291, 183: 0.0033, 184: 0.0019, 185: 0.0046, 186: -0.0112, 187: 0.0051, 188: -0.0141, 189: -0.0177, 190: 0.0093, 191: 0.0111, 192: 0.022, 193: 0.0019, 194: 0.0138, 195: -0.0057, 196: -0.0171, 197: 0.0178, 198: -0.0032, 199: -0.0127, 200: 0.0041, 201: -0.0058, 202: -0.0157, 7884: 1.0, 8531: 1.0}))]




```python
papers_meta_transformed.repartition(12,'features').write.parquet("./Dataset/papersmeta_transformed")
```


```python
papers_meta_transformed.show(5)
```

    +--------------------+--------------------+--------------------+---------+------------+-------------+--------------------+--------------------+-------------+-------------+-------------------+-------------------+--------------------+
    |            Paper_ID|             authors|             journal|wordcount|publish_Year|publish_Month|       word2vec_body|      word2vec_title|authors_index|journal_index|    authors_encoded|    journal_encoded|            features|
    +--------------------+--------------------+--------------------+---------+------------+-------------+--------------------+--------------------+-------------+-------------+-------------------+-------------------+--------------------+
    |0d3951ca998dcf8af...|Veir, Julia K.; L...|Veterinary Clinic...|     2490|        2010|           11|[0.06525062580420...|[0.00330294785089...|       1449.0|        113.0|(8321,[1449],[1.0])| (2750,[113],[1.0])|(11274,[0,1,2,3,4...|
    |10218d6165dcbaa34...|Engström, Patrik;...|       Nat Microbiol|     5389|        2019|           10|[0.12919333469184...|[-0.0156040839663...|       6916.0|        479.0|(8321,[6916],[1.0])| (2750,[479],[1.0])|(11274,[0,1,2,3,4...|
    |1b59e2160f61456f3...|Parashar, Bhupesh...|              Cureus|     2854|        2020|            5|[0.08179591124634...|[0.02742541182841...|       6954.0|        120.0|(8321,[6954],[1.0])| (2750,[120],[1.0])|(11274,[0,1,2,3,4...|
    |204e90c2972ccfb55...|Rong, Q.; Alexand...|          Arch Virol|     2549|        2003|            1|[0.12304916116595...|[-0.0329982108669...|        630.0|          3.0| (8321,[630],[1.0])|   (2750,[3],[1.0])|(11274,[0,1,2,3,4...|
    |20bb0f949ada4e02d...|Musselwhite, Char...|     J Transp Health|     1233|        2020|            4|[0.06484576156660...|[0.00330294785089...|       5787.0|       2614.0|(8321,[5787],[1.0])|(2750,[2614],[1.0])|(11274,[0,1,2,3,4...|
    +--------------------+--------------------+--------------------+---------+------------+-------------+--------------------+--------------------+-------------+-------------+-------------------+-------------------+--------------------+
    only showing top 5 rows
    


## Step 6 (PCA and Clustering)

### Apply the PCA


```python
papers_meta_transformed = spark.read\
            .format("parquet")\
            .option("header", "true")\
            .load("./Dataset/papersmeta_transformed")
```


```python
from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors

pca = PCA(k=2, inputCol="features", outputCol="features_pca")
model = pca.fit(papers_meta_transformed)
papers_meta_transformed = model.transform(papers_meta_transformed)
```


```python
model.explainedVariance
```




    DenseVector([0.9752, 0.0248])




```python
papers_meta_transformed.show(10)
```

    +--------------------+----------------+---------+------------+-------------+--------------------+--------------------+-------------+-------------+-----------------+-----------------+--------------------+--------------------+
    |             authors|         journal|wordcount|publish_Year|publish_Month|       word2vec_body|      word2vec_title|authors_index|journal_index|  authors_encoded|  journal_encoded|            features|        features_pca|
    +--------------------+----------------+---------+------------+-------------+--------------------+--------------------+-------------+-------------+-----------------+-----------------+--------------------+--------------------+
    |Wu, Peng; Wang, L...|        PLoS One|     1647|        2015|            9|[-0.0904070834538...|[-0.0057946556106...|        545.0|          1.0|(823,[545],[1.0])|  (533,[1],[1.0])|(1559,[0,1,2,3,4,...|[-1651.0701993916...|
    |                    |                |     2836|           0|            0|[0.04564585316306...|[-0.0129103393092...|          0.0|          0.0|  (823,[0],[1.0])|  (533,[0],[1.0])|(1559,[0,3,4,5,6,...|[-2835.9942152458...|
    |Herrera-Uribe, Jú...|         Vet Res|     3213|        2018|            9|[0.05120600047587...|[-0.0038783300106...|        428.0|         30.0|(823,[428],[1.0])| (533,[30],[1.0])|(1559,[0,1,2,3,4,...|[-3217.0730669380...|
    |                    |       J Exp Med|     1721|        1981|            4|[0.02892492618094...|[-0.0048593597725...|          0.0|        100.0|  (823,[0],[1.0])|(533,[100],[1.0])|(1559,[0,1,2,3,4,...|[-1725.0011497878...|
    |                    |                |     2332|           0|            0|[0.12313955832726...|[-0.0028581638947...|          0.0|          0.0|  (823,[0],[1.0])|  (533,[0],[1.0])|(1559,[0,3,4,5,6,...|[-2331.9952478303...|
    |Noh, Ji Yun; Song...|        PLoS One|     1141|        2013|            5|[-0.0906738900065...|[-0.0060536325327...|        546.0|          1.0|(823,[546],[1.0])|  (533,[1],[1.0])|(1559,[0,1,2,3,4,...|[-1145.0670399084...|
    |Lam, Wai-Yip; Yeu...|   BMC Microbiol|     2759|        2013|            5|[0.03766653158504...|[-0.0083344030523...|        342.0|         60.0|(823,[342],[1.0])| (533,[60],[1.0])|(1559,[0,1,2,3,4,...|[-2763.0637368799...|
    |Koonin, Eugene V....|        Virology|    10295|        2015|            5|[0.04314724207959...|[-0.0034523240841...|        411.0|          2.0|(823,[411],[1.0])|  (533,[2],[1.0])|(1559,[0,1,2,3,4,...|[-10299.052376958...|
    |McKimm-Breschkin,...|Treat Respir Med|     3301|        2012|            8|[-0.0541274390783...|[-0.0091846164141...|        257.0|        154.0|(823,[257],[1.0])|(533,[154],[1.0])|(1559,[0,1,2,3,4,...|[-3305.0607183392...|
    |                    |                |      653|           0|            0|[-0.0326838513918...|[-0.0066625987264...|          0.0|          0.0|  (823,[0],[1.0])|  (533,[0],[1.0])|(1559,[0,3,4,5,6,...|[-652.99867743813...|
    +--------------------+----------------+---------+------------+-------------+--------------------+--------------------+-------------+-------------+-----------------+-----------------+--------------------+--------------------+
    only showing top 10 rows
    



```python
papers_meta_transformed.repartition(12).write.parquet("./Dataset/papersmeta_transformed_pca")
```

### Define the clustering model

### Choose number of clusters k (based on Elbow method and silhouette_score)


```python
papers_meta_transformed = spark.read\
            .format("parquet")\
            .option("header", "true")\
            .load("./Dataset/papersmeta_transformed_pca")
```


```python
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Calculate cost and plot
import numpy as np
import pandas as pd 

cost = np.zeros(15)
silhouette = np.zeros(15)

for k in range(2,15):
    kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol('features_pca')
    model = kmeans.fit(papers_meta_transformed)
    cost[k] = model.computeCost(papers_meta_transformed)
    clusterdData = model.transform(papers_meta_transformed)
    evaluator = ClusteringEvaluator()
    silhouette[k] = evaluator.evaluate(clusterdData)

# Plot the cost
df_eval = pd.DataFrame(np.array([cost[2:].tolist(),silhouette[2:].tolist()])).transpose()
df_eval.columns = ["cost", "silhouette_score"]
new_col = [2,3,4,5,6,7,8,9,10,11,12,13,14]
df_eval.insert(0, 'cluster', new_col)

```


```python
import seaborn as sns
import matplotlib.pyplot as plt
sns.set_style("whitegrid")
fig = plt.figure(figsize=(40,10))
ax1 = fig.add_subplot(1, 2, 1)
ax1.set(xticks=range(2,15))
ax2 = fig.add_subplot(1, 2, 2)
ax2.set(xticks=range(2,15))
sns.lineplot(x='cluster', y='cost', data=df_eval, ax=ax1)
sns.lineplot(x='cluster', y='silhouette_score', data=df_eval, ax=ax2)
```




    <matplotlib.axes._subplots.AxesSubplot at 0x7f5224ff6510>




![png](output_116_1.png)



```python
df_eval
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>cluster</th>
      <th>cost</th>
      <th>silhouette_score</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2</td>
      <td>1.007589e+11</td>
      <td>0.993991</td>
    </tr>
    <tr>
      <th>1</th>
      <td>3</td>
      <td>6.781246e+10</td>
      <td>0.993733</td>
    </tr>
    <tr>
      <th>2</th>
      <td>4</td>
      <td>4.974666e+10</td>
      <td>0.987616</td>
    </tr>
    <tr>
      <th>3</th>
      <td>5</td>
      <td>3.020390e+10</td>
      <td>0.785008</td>
    </tr>
    <tr>
      <th>4</th>
      <td>6</td>
      <td>2.015361e+10</td>
      <td>0.685087</td>
    </tr>
    <tr>
      <th>5</th>
      <td>7</td>
      <td>1.719920e+10</td>
      <td>0.691826</td>
    </tr>
    <tr>
      <th>6</th>
      <td>8</td>
      <td>1.511783e+10</td>
      <td>0.685413</td>
    </tr>
    <tr>
      <th>7</th>
      <td>9</td>
      <td>1.087302e+10</td>
      <td>0.700327</td>
    </tr>
    <tr>
      <th>8</th>
      <td>10</td>
      <td>8.321896e+09</td>
      <td>0.674909</td>
    </tr>
    <tr>
      <th>9</th>
      <td>11</td>
      <td>7.763562e+09</td>
      <td>0.710722</td>
    </tr>
    <tr>
      <th>10</th>
      <td>12</td>
      <td>5.943687e+09</td>
      <td>0.704555</td>
    </tr>
    <tr>
      <th>11</th>
      <td>13</td>
      <td>5.312780e+09</td>
      <td>0.704179</td>
    </tr>
    <tr>
      <th>12</th>
      <td>14</td>
      <td>4.317527e+09</td>
      <td>0.708570</td>
    </tr>
  </tbody>
</table>
</div>



### We choose k=4 based on the plots above


```python
kmeans = KMeans().setK(4).setFeaturesCol('features_pca')
model = kmeans.fit(papers_meta_transformed)
clusterdData = model.transform(papers_meta_transformed)
```

## Step 7 Recommender System

The goal here is to build a basic recommender the system that reccomends similar papers to a given title. When provided with a paper title, the recommender is only going to consider papers which belong to the same cluster. it will then run cosine similiraty between the given title and the processed paper titles in the database. it will return a dictionary with suggested paper titles and the cosine dot product with respect to the given title


```python
paper_titles_df = spark.read\
            .format("parquet")\
            .option("header", "true")\
            .load("./Dataset/papers_meta_word2vec").select('Paper_ID', 'title', 'title_clean')
clusterdData = paper_titles_df.join(clusterdData, on=['Paper_ID'], how='inner')
clusterdData.show(5)
```

    +--------------------+--------------------+--------------------+--------------------+--------------------+---------+------------+-------------+--------------------+--------------------+-------------+-------------+-------------------+-------------------+--------------------+--------------------+----------+
    |            Paper_ID|               title|         title_clean|             authors|             journal|wordcount|publish_Year|publish_Month|       word2vec_body|      word2vec_title|authors_index|journal_index|    authors_encoded|    journal_encoded|            features|        features_pca|prediction|
    +--------------------+--------------------+--------------------+--------------------+--------------------+---------+------------+-------------+--------------------+--------------------+-------------+-------------+-------------------+-------------------+--------------------+--------------------+----------+
    |0782b4fb23ca65baf...|the population ge...|[population, gene...|Tibayrenc, Michel...|        Acta Tropica|     3686|        2015|           11|[0.02713752761508...|[0.01237550669466...|       4017.0|       2721.0|(8321,[4017],[1.0])|(2750,[2721],[1.0])|(11274,[0,1,2,3,4...|[-3692.4819062727...|         0|
    |14d8fd027f39c8311...|drug induced dela...|[drug, induced, d...|Klimas, Natasha; ...|Cutaneous Drug Er...|     2594|        2015|            5|[0.04067398937230...|[-0.0022071010898...|       6489.0|       1331.0|(8321,[6489],[1.0])|(2750,[1331],[1.0])|(11274,[0,1,2,3,4...|[-2600.4873834375...|         0|
    |1e5228e3f0658479a...|canonicalizing kn...|[canonicalizing, ...|Fatma, Nausheen; ...|Advances in Knowl...|     2101|        2020|            4|[0.06466745659127...|[0.00304986406117...|       4030.0|         40.0|(8321,[4030],[1.0])|  (2750,[40],[1.0])|(11274,[0,1,2,3,4...|[-2107.5060457725...|         0|
    |1e663ac169e08ea02...|the effects of a ...|[effects, celebri...|Yip, Paul S.F.; F...|Journal of Affect...|     1177|        2006|            7|[0.07743242526835...|[0.00422781358273...|       1147.0|       1477.0|(8321,[1147],[1.0])|(2750,[1477],[1.0])|(11274,[0,1,2,3,4...|[-1183.4657902211...|         0|
    |1f26b5e8291ea1ddc...|the case for labo...|[case, laboratory...|Kaul, Karen L.; S...|         Acad Pathol|     7361|        2017|            7|[0.03405737735745...|[0.03623301810067...|       8216.0|       1072.0|(8321,[8216],[1.0])|(2750,[1072],[1.0])|(11274,[0,1,2,3,4...|[-7367.4690924711...|         3|
    +--------------------+--------------------+--------------------+--------------------+--------------------+---------+------------+-------------+--------------------+--------------------+-------------+-------------+-------------------+-------------------+--------------------+--------------------+----------+
    only showing top 5 rows
    


### We will pre-calculate tf and idf values and store them in a dataframe named data for our recommender system.


```python
from pyspark.sql.functions import udf, col
df_recommender = clusterdData.select('title', 'title_clean', col('prediction').alias('cluster'))
from pyspark.ml.feature import HashingTF, IDF
hashingTF = HashingTF(inputCol="title_clean", outputCol="tf")
tf = hashingTF.transform(df_recommender)

idf = IDF(inputCol="tf", outputCol="idf_feature").fit(tf)
tfidf = idf.transform(tf)
```


```python
from pyspark.ml.feature import Normalizer
normalizer = Normalizer(inputCol="idf_feature", outputCol="norm")
data = normalizer.transform(tfidf)
```

### We will define the recommendPaper function. it takes a paper title and returns N recommendations from the same cluster.


```python
from pyspark.sql.types import DoubleType

dot_udf = udf(lambda x,y: float(x.dot(y)), DoubleType())

def recommendPaper(paper_title,N, data=data):
    target_paper = data.filter(data['title'] == paper_title)
    input_cluster = target_paper.select('cluster').collect()[0].cluster
    data = data.filter(data['cluster'] == input_cluster)
    recommendations = target_paper.alias("tearget_paper").crossJoin(data.alias("right"))\
        .select(col("tearget_paper.title").alias("target_title"), 
            col("right.title").alias("recommended_title"), 
            dot_udf("tearget_paper.norm", "right.norm").alias("dot"))\
        .sort(col("dot").desc())\
        .limit(N+1)
    return {reccomendation.recommended_title:reccomendation.dot for reccomendation in recommendations.collect()[1:]}
```


```python
recommendPaper('the population genetics of trypanosoma cruzi revisited in the light of the predominant clonal evolution model',4)
```




    {'effect of the plasmid dna vaccination on macroscopic and microscopic damage caused by the experimental chronic trypanosoma cruzi infection in the canine model': 0.2633751212982291,
     'ccr plays a critical role in the development of myocarditis and host protection in mice infected with trypanosoma cruzi': 0.2627314956954824,
     'renin angiotensin system revisited': 0.23883130343223866,
     'evolution of viruses': 0.1836250991049867}




```python
recommendPaper('annual update in intensive care and emergency medicine',2)
```




    {'annual update in intensive care and emergency medicine annual update in intensive care and emergency medicine': 0.9999999999999997,
     'annual update in intensive care and emergency medicine': 0.9999999999999997}




```python
recommendPaper('use of simple laboratory features to distinguish the early stage of severe acute respiratory syndrome from dengue fever',3)
```




    {'severe acute respiratory syndrome': 0.3645151517477996,
     'severe acute respiratory syndrome sars ': 0.3332956079500218,
     'a simple laboratory parameter facilitates early identification of covid patients': 0.3125342703495677}




```python
recommendPaper('virology of hepatitis c virus',10)
```




    {'hepatitis c virus rna replication': 0.5709986104132124,
     'hepatitis e a disease of reemerging importance basic virology and epidemiology of hepatitis e virus recent developments in hepatitis e': 0.44891119168862215,
     'glycyrrhizin as antiviral agent against hepatitis c virus': 0.4175231102580461,
     'amantadine in treatment of chronic hepatitis c virus infection ': 0.4156410776595072,
     'inhibition of hepatitis c virus replication by chloroquine targeting virus associated autophagy': 0.37790189679458763,
     'towards a small animal model for hepatitis c': 0.37450913307191663,
     'archives of virology mouse hepatitis virus nasoencephalopathy is dependent upon virus strain and host genotype': 0.3682394327277896,
     'by mouse hepatitis virus type ': 0.35835528698460356,
     'the role of type iii interferons in hepatitis c virus infection and therapy': 0.354883424906226,
     'characterization of antibodies induced by vaccination with hepatitis c virus envelope glycoproteins': 0.3543678425480336}


