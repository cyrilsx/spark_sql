from pyparsing import col
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType


def init_spark_session():
    return SparkSession.builder.appName("test").getOrCreate()


def test(session, f, prefix):
    colName = f.name
    if prefix is not None:
        colName = prefix + "." + f.name

    if f.dataType == StructType:
        return flattenSchema(session, f.dataType, colName)
    else:
        return [col(colName)]

def flattenSchema(session, schema: StructType, prefix: str):
    session.sparkContext.parallelize(schema.fields).flatMap(lambda f: test(f, prefix))


if __name__ == '__main__':
    session = init_spark_session()
    prize = session.read.json("ds/prize.json").select(f.explode("prizes"))
    laureate = session.read.json("ds/laureate.json").select(f.explode("laureates"))

    prize.printSchema()
    prize.select("col").show()
    prizeid_year = prize.select(f.explode("col.laureates.id").alias("person_id"), "col.year") #.where(f.isnull("col.laureates.id")).show()
    prizeid_year.printSchema()

    join =prizeid_year.join(laureate, prizeid_year["person_id"] == laureate["col.id"])
    join.printSchema()

    join.select("year", "col.bornCountry").orderBy(f.desc("year")).groupBy("bornCountry").count().show()
        #.groupBy("col.bornCountry").count().show()