# HDFS-ES

Hdfs-es - spark job that gets one or multiple files from hadoop file infrastructure and export them to elasticsearch.

## Features

- Ability to run script both: locally and using yarn manager
- Reading one or multiple HDFS files with any number of partitions
- Configurable elasticsearch parameters: node, port, resource(index/type)
- CSV files support
- Data schema applied dynamically from user specified json file
- Ability to specify new column "origin" values for data distinction from other executions
