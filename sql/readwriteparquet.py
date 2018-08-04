from pyspark.sql import SparkSession


def basic_datasource_example(spark):
    df = spark.read.load("D:\\workspace\\pyspark-learn\\resources\\employees.json", format="json")
    df.show()
    df.write.parquet('D:\\workspace\\pyspark-learn\\resources\\employee.parquet')


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("Python Spark SQL data source example") \
        .getOrCreate()

    basic_datasource_example(spark)

    spark.stop()
