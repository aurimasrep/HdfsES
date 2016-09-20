#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       		: hdfs-es.py
Author     		: Aurimas Repecka <aurimas.repecka AT gmail dot com>
Description     : Transfers data from hdfs to elasticsearch
"""

# system modules
import os
import sys
import argparse
import ConfigParser

from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField, StringType, BooleanType
from pyspark.sql.functions import lit

from datetime import datetime as dt
from datetime import timedelta

# elasticsearch node
CONFIG_PATH = os.path.join(os.environ.get('HDFSES_CONFIG', '/'), "hdfs-es.cfg")
# data schema path
SCHEMA_PATH = os.path.join(os.environ.get('HDFSES_SCHEMA', '/'), "schema.json")

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--fname", action="store",
            dest="fname", default="", help="Input data file on HDFS, e.g. hdfs:///path/data/file")
        self.parser.add_argument("--yarn", action="store_true",
            dest="yarn", default=False, help="Be yarn")
        self.parser.add_argument("--basedir", action="store",
            dest="basedir", default="/cms/phedex-monitoring/", help="Base directory of hdfs files")
        self.parser.add_argument("--fromdate", action="store",
            dest="fromdate", default="", help="Filter by start date")
        self.parser.add_argument("--todate", action="store",
            dest="todate", default="", help="Filter by end date")
        self.parser.add_argument("--logs", action="store",
            dest="logs", default="INFO", help="Set log level")
        self.parser.add_argument("--esorigin", action="store",
            dest="esorigin", default="custom", help="Writes an data origin field to elastic search")


def schema():
    """
    Provides schema (names, types, nullable) for csv snapshot

    :returns: StructType consisting StructField array
    """
    return StructType([StructField("now_sec", DoubleType(), True),
                     StructField("dataset_name", StringType(), True),
                     StructField("dataset_id", IntegerType(), True),
                     StructField("dataset_is_open", StringType(), True),
                     StructField("dataset_time_create", DoubleType(), True),
                     StructField("dataset_time_update", DoubleType(), True),
                     StructField("block_name", StringType(), True), 
                     StructField("block_id", IntegerType(), True),
                     StructField("block_files", IntegerType(), True),
                     StructField("block_bytes", DoubleType(), True),
                     StructField("block_is_open", StringType(), True),
                     StructField("block_time_create", DoubleType(), True),
                     StructField("block_time_update", DoubleType(), True),
                     StructField("node_name", StringType(), True),
                     StructField("node_id", IntegerType(), True),
                     StructField("br_is_active", StringType(), True),
                     StructField("br_src_files", IntegerType(), True),
                     StructField("br_src_bytes", DoubleType(), True),
                     StructField("br_dest_files", IntegerType(), True),
                     StructField("br_dest_bytes", DoubleType(), True),
                     StructField("br_node_files", IntegerType(), True),
                     StructField("br_node_bytes", DoubleType(), True),
                     StructField("br_xfer_files", IntegerType(), True),
                     StructField("br_xfer_bytes", DoubleType(), True),
                     StructField("br_is_custodial", StringType(), True),
                     StructField("br_user_group_id", IntegerType(), True),
                     StructField("replica_time_create", DoubleType(), True),
                     StructField("replica_time_updater", DoubleType(), True)])

def getFileList(basedir, fromdate, todate):
    """
    Finds snapshots in given directory by interval dates

    :param basedir: directory where snapshots are held
    :param fromdate: date from which snapshots are filtered
    :param todate: date until which snapshots are filtered
    :returns: array of filtered snapshots paths
    :raises ValueError: if unparsable date format
    """
    dirs = os.popen("hadoop fs -ls %s | sed '1d;s/  */ /g' | cut -d\  -f8" % basedir).read().splitlines()
    # if files are not in hdfs --> dirs = os.listdir(basedir)

    try:
        fromdate = dt.strptime(fromdate, "%Y-%m-%d")
        todate = dt.strptime(todate, "%Y-%m-%d")
    except ValueError as err:
        raise ValueError("Unparsable date parameters. Date should be specified in form: YYYY-mm-dd")		
 		
    pattern = re.compile(r"(\d{4}-\d{2}-\d{2})")

    dirdate_dic = {}
    for di in dirs:
        matching = pattern.search(di)
        if matching:
            dirdate_dic[di] = dt.strptime(matching.group(1), "%Y-%m-%d")

    # if files are not in hdfs --> return [ basedir + k for k, v in dirdate_dic.items() if v >= fromdate and v <= todate]
    return [k for k, v in dirdate_dic.items() if v >= fromdate and v <= todate]		


def validateLogLevel(log_level):
    """
    Validates user specified spark log level

    :param log_level: string of log level
    :raises ValueError: if log level is not in LOGLEVELS list
    """
    if log_level not in LOGLEVELS:
        raise ValueError("Specified log level = %s not available" % log_level)


def validateEsParams(node, port, resource):
    """
    Validates user specified Elasticsearch parameters

    :param node: string representation of elasticsearch node
    :param port: string representation of elasticearch port
    :param resource string representation of elasticsearch index/type
    :raises ValueError: if Elasticsearch node or port were not specified or index/type was not in correct form
    """
    if not node:
        raise ValueError("Elasticsearch node was not specified")
    if not port:
        raise ValueError("Elasticsearch port was not specified")
    if len(resource.split('/')) != 2:
        raise ValueError("Elasticsearch index/type was not provided in the correct form")

def defDates(fromdate, todate):
    """
    Check if dates are specified and returns default values

    :param fromdate: interval beggining date
    :param todate: interval end date
    :returns: tuple of from and to dates
    """
    if not fromdate or not todate:
        fromdate = dt.strftime(dt.now(), "%Y-%m-%d")
        todate = dt.strftime(dt.now(), "%Y-%m-%d")
    return fromdate, todate

def unionAll(dfs):
    """
    Unions snapshots in one dataframe	

    :param item: list of dataframes
    :returns: union of dataframes
    """
    return reduce(DataFrame.unionAll, dfs)		


#########################################################################################################################################

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()

    # reading elasticsearch configuration
    config = ConfigParser.ConfigParser()
    config.read(CONFIG_PATH)
    esnode = config.get('ElasticSearch','node')
    esport = config.get('ElasticSearch','port')
    esresource = config.get('ElasticSearch','resource')

    # setup spark/sql context to be used for communication with HDFS
    sc = SparkContext(appName="hdfs-es")

    # setting spark log level
    logs = opts.logs.upper()
    validateLogLevel(logs)
    sc.setLogLevel(logs)

    # setting up spark sql variables
    sqlContext = HiveContext(sc)
    schema_def = schema()

    # read given file(s) into dataframe
    if opts.fname:
        pdf = sqlContext.read.format('com.databricks.spark.csv')\
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(opts.fname, schema = schema_def)
    elif opts.basedir:
        fromdate, todate = defDates(opts.fromdate, opts.todate)
        files = getFileList(opts.basedir, fromdate, todate)
        msg = "Between dates %s and %s found %d directories" % (fromdate, todate, len(files))
        print msg

        if not files:
            return
        pdf = unionAll([sqlContext.read.format('com.databricks.spark.csv')
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(file_path, schema = schema_def) \
                        for file_path in files])
    else:
        raise ValueError("File or directory not specified. Specify fname or basedir parameters.")

    # write data to elasticsearch
    validateEsParams(esnode, esport, esresource)
    aggres = aggres.withColumn("origin", lit(opts.esorigin))
    aggres.repartition(1).write.format("org.elasticsearch.spark.sql").option("es.nodes", esnode)\
                                                                     .option("es.port", esport)\
                                                                     .option("es.resource", esresource)\
                                                                     .save(mode="append")

if __name__ == '__main__':
    main()

