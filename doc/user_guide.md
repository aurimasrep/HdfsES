# User guide 

## Parameters

### fname

Fname is used fo specifying one input data file in hdfs. If not specified assumption is made that user uses script in multi-file mode, so the parameter basedir expected to be defined. If none of these parameters (fname, basedir) are defined user gets an error.

### basedir

Basedir is used for specifying directory in which hdfs files are held. Files in the directory suppose to have date (YYYY-mm-dd) string in their names. Parameter is used along with parameters fromdate and todate to filter files that are going to be processed. If either: fromdate or todate is not specified default value: now() is set for both of these variables, so only today snapshot is being processed.

### fromdate

Fromdate is used for specifying date from which hdfs files in basedir will be processed. Date format is expected to be YYYY-mm-dd. If not - user gets an error. If not specified default value: now() is set.

### todate

Tdate is used for specifying date until which hdfs files in basedir will be processed. Date format is expected to be YYYY-mm-dd. If not - user gets an error. If not specified default value: now() is set.

### yarn

Yarn is used for activitacing Yarn cluster management technology.

### logs

Logs is used for specifying log level that spark produces during the execution. User must choose from pre-specified options otherwise he gets an error.
```
ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
```

### esorigin

Esorigin is used for specifying the origin of data. It basically adds new column and fill with data specified in this parameter (it is done only for data that goes to elasticsearch, hdfs data remains untouched) .When using as a cronjob it should have value of "cronjob". Running script manually user should specify origin by himself (or leave empty - origin "custom"). This field should be later used for making searches in kibana (to select proper data, ex.: origin : cronjob).

### Environment variables
- SPARK_CSV_ASSEMBLY_JAR - spark-csv package path
- ES_HADOOP_JAR - elasticsearch-hadoop package path
- PYTHONPATH - path to projects python folder. Ex.: ~/src/python
- HDFSES_SCHEMA - path to data schema (schema.json). Ex.: ~/data
- HDFSES_CONFIG - path to elasticsearch configuration (hdfs-es.cfg). Ex.: ~/etc

```
bash pbr.sh --yarn \
		--basedir hdfs:///project/awg/cms/phedex/block-replicas-snapshots/csv/ \
		--fromdate 2015-08-04 \
		--todate 2015-08-09 \
		--esorigin cronjob
		#--logs INFO
		#--fname hdfs:///project/awg/cms/phedex/block-replicas-snapshots/csv/time=2016-07-09_03h07m28s 
```
