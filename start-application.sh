#!/bin/bash

if [ ${ENABLE_DEV_REST_API} ]; then
  java -Dspring.profiles.active=dev -jar /usr/app/transitdata-pubtrans-source.jar
  else
  java -jar /usr/app/transitdata-pubtrans-source.jar
fi