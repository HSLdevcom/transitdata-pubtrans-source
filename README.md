## Description

A proof of concept for following functionalities
- Connect to Pubtrans ptROI and ptDOI4
- Fetch rows of data from different tables
- Connect to a local Pulsar cluster
- Produce data to pulsar topics

More system-level documentation can be found in [this project](https://gitlab.hsl.fi/transitdata/transitdata-doc).

## Building

### Dependencies

This project depends on [transitdata-common](https://github.com/HSLdevcom/transitdata-common) project.

Either use released versions from public maven repository or build your own and install to local maven repository:
  - ```cd transitdata-common && mvn install```  

### Locally

- ```mvn compile```  
- ```mvn package```  

### Docker image

- Run [this script](build-image.sh) to build the Docker image


## Running

Requirements:
- Local Pulsar Cluster
  - By default uses localhost, override host in PULSAR_HOST if needed.
    - Tip: f.ex if running inside Docker in OSX set `PULSAR_HOST=host.docker.internal` to connect to the parent machine
  - You can use [this script](https://gitlab.hsl.fi/transitdata/transitdata-doc/bin/pulsar/pulsar-up.sh) to launch it as Docker container
- Connection string to Pubtrans database is read from file. 
  - Set filepath via env variable FILEPATH_CONNECTION_STRING, default is `/run/secrets/pubtrans_community_conn_string` 

All other configuration options are configured in the [config files](src/main/resources/) 
which can also be configured externally via env variable CONFIG_PATH. 

Launch Docker container with

```docker-compose -f compose-config-file.yml up <service-name>```   

See [the documentation-project](https://gitlab.hsl.fi/transitdata/transitdata-doc) for details
