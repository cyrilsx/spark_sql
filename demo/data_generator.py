import json

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as fn
import random
import sys



def generate():
    ss = SparkSession.builder.appName("generator").getOrCreate()
    df = ss.read.json("../ds/person.json")
    resultsDf = df.select("results")

    sc = SparkContext()

    sc.parallelize()

    resultsDf.printSchema()
    nestedArray = resultsDf.select(fn.explode('results'))
    nestedArray.select("col.*").show()
    dict = nestedArray.select("col.id", "col.name")\
        .rdd \
        .map(lambda e: {"id": e.id, "name": e.name, "birth_year":random.randint(1960, 2000), "hometown": "NA"})\
        .collect()

    json.dump(dict, open("../ds/profile.json", "w"))


if __name__ == '__main__':
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
    generate()


