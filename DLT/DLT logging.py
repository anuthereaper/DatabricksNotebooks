# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from datetime import datetime

import dlt

import logging

def load_table_to_bronze(tablename):
    @dlt.table(name=f"bronze_{tablename}",comment=f"This is a bronze table bronze_{tablename}")
    def incremental_bronze():
        logger.info("Reading data from the source table.")
        df = spark.readStream.table(f"cust")
        logger.info(f"Number of rows read: {df.count()}")
        return df

load_table_to_bronze("myDLTtable")
