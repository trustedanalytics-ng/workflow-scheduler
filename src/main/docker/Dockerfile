FROM tap-base-java:java8-jessie
VOLUME /tmp
ADD workflow-scheduler-*.jar app.jar
RUN sh -c 'touch /app.jar'
ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/app.jar"]
