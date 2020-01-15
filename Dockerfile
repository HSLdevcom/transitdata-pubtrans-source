FROM openjdk:8-jre-slim

#Install curl for health check
RUN apt-get update && apt-get install -y --no-install-recommends curl

#This container can access the build artifacts inside the BUILD container.
#Everything that is not copied is discarded
COPY target/transitdata-pubtrans-source.jar /usr/app/transitdata-pubtrans-source.jar
COPY start-application.sh /start-application.sh
RUN chmod +x /start-application.sh
ENTRYPOINT ["/start-application.sh"]
