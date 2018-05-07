from IPython.core.pylabtools import figsize
from ipywidgets import fixed
from pyspark import Row
from pyspark.context import SparkContext
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as fn


import pandas as pd
#pd.set_option('display.mpl_style', 'default') # Make the graphs a bit prettier
figsize(15, 5)




class Page:

    def __init__(self, id, title, content):
        self.id = id
        self.title = title
        self.content = content


class UserVisit:

    def __init__(self, username, page, nb_visit):
        self.username=username
        self.page=page
        self.nb_visit=nb_visit



def get_spark_session():
    return SparkSession.builder.appName("test").getOrCreate()

def get_spark_context():
    return SparkContext()

def create_reflexion_df():
    sc = get_spark_context()
    people = sc.parallelize([
        ("Bib", 17, "Geneva"),
        ("Cyril", 25, "Gland"),
        ("Jean", 63, "Stockholm"),
        ("Kevin", 12, "Geneva"),
        ("Maya", 3, "Lyon")])\
        .map(lambda p: Row(name=p[0],age=p[1],hometown=p[1]))

    df = get_spark_session().createDataFrame(people)
    df.show()



def create_explicit_df():
    sc = get_spark_context()
    people = sc.parallelize([
        ("Bib", 17, "Geneva"),
        ("Cyril", 25, "Gland"),
        ("Jean", 63, "Stockholm"),
        ("Kevin", 12, "Geneva"),
        ("Maya", 3, "Lyon")
    ])

    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("hometown", StringType(), True),
    ])

    df = get_spark_session().createDataFrame(people, schema)
    df.show()


def load_data_json():
    session = get_spark_session()
    df = session.read.json("ds/profile.json")
    df.show()
    return df


def sql_queries():
    session = get_spark_session()
    df = session.read.json("ds/profile.json")
    df.createOrReplaceTempView("profile")
    session.sql("SELECT name, birth_year FROM profile WHERE profile.birth_year > 1985").show()
    session.sql("SELECT birth_year, count(1) FROM profile GROUP BY profile.birth_year HAVING COUNT(1) > 2 ORDER BY profile.birth_year DESC").show()


def select_df():
    session = get_spark_session()
    df_profile = session.read.json("ds/profile.json")
    df_profile.printSchema()
    #df_profile.select("name", "birth_year").show()

    #df_profile.select("name", "birth_year").where(df_profile.birth_year > 1986).show()
    # df_profile.select("name", "birth_year").where(df_profile.birth_year > 1986).orderBy(df_profile.birth_year).show()
    #df_profile.select("name", "birth_year").where(df_profile.birth_year > 1986).orderBy(fn.desc("birth_year")).show()
    df_profile.select(df_profile.name.alias("dude"), df_profile.birth_year).where(df_profile.birth_year < 1986).show()
    df_profile.select(fn.sum(df_profile.birth_year)).show()
    df_profile.groupBy(df_profile.name).sum().show()

def drop():
    session = get_spark_session()
    df_profile = session.read.json("ds/profile.json")
    df_profile.na.drop("all").show()


def to_pandas():
    session = get_spark_session()
    df_profile = session.read.json("ds/profile.json")
    panda = df_profile.toPandas()
    print(panda[:3])
    panda['birth_year'].plot()


def below_threshold(threshold, group="group", power="power"):
    @pandas_udf("struct<group: string, below_threshold: boolean>", PandasUDFType.GROUP_MAP)
    def below_threshold_(df):
        df = pd.DataFrame(
            df.groupby(group).apply(lambda x: (x[power] < threshold).any()))
        df.reset_index(inplace=True, drop=False)
        return df

    return below_threshold_


def fill():
    session = get_spark_session()
    df_profile = session.read.json("ds/profile.json")
    df_profile.na.fill(1).show()



if __name__ == '__main__':
    #create_reflexion_df()
    #create_explicit_df()
    #load_data_json()

    #sql_queries()

    #select_df()
    #agg

    drop()
    #fill()

    # bonus
    #to_pandas()