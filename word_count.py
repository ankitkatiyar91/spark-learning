import re

from pyspark.sql import SparkSession

from spark_session import get_spark_session


def word_count():
    spark: SparkSession = get_spark_session('word-count')
    df = spark.sparkContext.textFile('temp/**').flatMap(lambda line: line.split(" ")) \
        .map(lambda word: re.sub('[^A-Za-z0-9]+', '', word)).map(lambda word: (word, 1)).reduceByKey(
        lambda v1, v2: v1 + v2)
    print(df)

    print('Top 5\n', df.sortBy(lambda v: -v[1]).collect())
    spark.stop()


if __name__ == '__main__':
    word_count()
