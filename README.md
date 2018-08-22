## Description

A proof of concept for following functionalities
- Connect to ptROI and ptDOI4
- Fetch rows of data from different tables
- Connect to a local Pulsar cluster
- Produce data to pulsar topics

## Building

Run [this script](build-image.sh) to build the Docker image


## Running

Requirements:
- Local Pulsar Cluster
  - By default uses localhost, override host in PULSAR_HOST if needed.
    - Tip: f.ex if running inside Docker in OSX set `PULSAR_HOST=host.docker.internal` to connect to the parent machine
  - You can use [this script](../bin/pulsar/pulsar-up.sh) to launch it as Docker container
- Connection to Pubtrans SQL Server database
  - TODO Define host and credentials in secrets

Launch Docker container:

```docker-compose up pulsar-pubtrans-connect-arrival```   


## TODO

### Serialization ideas:
- Number of columns from ResultSetMetaData
- List of columns in config file
- These two are enough to pipe through raw string messages, even as JSON. This leads to numbers being strings in the final message. Is JSON the correct format? What about protobuf?
- Should the responsibility for the schema be on the consumer or producer side? Consumer needs to anyhow know the datatypes for each column. Is it better to parse strings to numbers on producer or consumer side?
- How are messages serialized?
  - One schema/protobuf/class per db table
  - Handler func reads rows from resultSet
  - Create a new pulsar message
  - Create a new protobuf message
  - Fill the class with data from the row
  - Build the class
  - Write the contents as byte[] (message.toByteArray) to the Pulsar message
  - Set the message key for the Pulsar message (row Id?)

### Performance/threading ideas:
- Every db query can be parallelized, and so can the handling of each message in each resultSet. Should everything jsut be lauched as a runnable task?
- How to configure number of threads?

### Utility
- Logging. At startup, at connections, at every fetched batch of data
- At startup, verify connections. Reconnect at intervals if db connection is lost
- One process per database table. Config file sets table and topic for producer. Specific handler method for each table. Causes duplicate work, but necessary
