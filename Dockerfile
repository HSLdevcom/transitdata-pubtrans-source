FROM eclipse-temurin:24-alpine
#Install curl for health check
RUN apk add --no-cache curl
ADD target/transitdata-pubtrans-source.jar /usr/app/transitdata-pubtrans-source.jar
COPY start-application.sh /
RUN chmod +x /start-application.sh
CMD ["/start-application.sh"]
