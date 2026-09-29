"""Shared helpers so every day's script stays short."""
import pathlib
from pyspark.sql import SparkSession

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = str(ROOT / "data")
OUT = str(ROOT / "output")


def get_spark(app_name: str = "pyspark-30-days") -> SparkSession:
    """Local SparkSession with small settings suitable for laptop practice."""
    spark = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
