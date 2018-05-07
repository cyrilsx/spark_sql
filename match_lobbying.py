from pyspark.sql import SparkSession


def get_spark_session():
    return SparkSession.builder.appName("generator").getOrCreate()


def go():
    session = get_spark_session()

    pDF = session.read.json("../ds/person.json")


if __name__ == '__main__':
    go()