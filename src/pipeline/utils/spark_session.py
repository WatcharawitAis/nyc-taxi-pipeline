"""This file for spark session creation"""

from pyspark.sql import SparkSession

SPARK = SparkSession.getActiveSession()
