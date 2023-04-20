# Transitdata-pubtrans-source [![Test and create Docker image](https://github.com/HSLdevcom/transitdata-pubtrans-source/actions/workflows/test-and-build.yml/badge.svg)](https://github.com/HSLdevcom/transitdata-pubtrans-source/actions/workflows/test-and-build.yml)

This application is used for querying stop time estimates from PubTrans DB and publishing them to a Pulsar topic.

This project is part of the [Transitdata Pulsar-pipeline](https://github.com/HSLdevcom/transitdata).

## Description

Application functionality:
- Connect to Pubtrans ptROI and ptDOI4
- Fetch rows of data from different tables
- Include additional journey-related metadata from Redis
- Connect to a local Pulsar cluster
- Produce data to a Pulsar topic

## Building

### Dependencies

This project depends on [transitdata-common](https://github.com/HSLdevcom/transitdata-common) project.

### Locally

- `mvn compile`
- `mvn package`

### Docker image

- Run [this script](build-image.sh) to build the Docker image

## Running

### Dependencies

* Pulsar
  * You can use [this script](https://github.com/HSLdevcom/transitdata/blob/master/bin/pulsar/pulsar-up.sh) to launch it as Docker container
* Connection to a PubTrans DB
  * Write JDBC connection string to a file and point to that file with environment variable `FILEPATH_CONNECTION_STRING`

### Environment variables

* `REDIS_CONN_TIMEOUT_SECS`: Redis connection timeout
* `REDIS_HEALTH_CHECK_ENABLED`: whether to enable Redis healthcheck
* `DB_TABLE`: database table name to be queried. The default should work for production DB
* `PUBTRANS_DATA_TIMEZONE`: timezone which is used in PubTrans
* `PUBTRANS_QUERY_TIMEOUT`: timeout for database queries
* `PUBTRANS_NETWORK_TIMEOUT`: timeout for database network operations
* `ENABLE_CACHE_TIMESTAMP_CHECK`: whether to check the cache update timestamp in Redis
* `CACHE_MAX_AGE_IN_MINS`: maximum age for cache contents
