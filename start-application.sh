#!/bin/sh

if [[ "${DEBUG_ENABLED}" = true ]]; then
  java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -XX:InitialRAMPercentage=10.0 -XX:MaxRAMPercentage=95.0 -jar /usr/app/transitdata-pubtrans-source.jar
else
  java -XX:InitialRAMPercentage=10.0 -XX:MaxRAMPercentage=95.0 -jar /usr/app/transitdata-pubtrans-source.jar
fi
