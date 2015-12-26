FROM jeanblanchard/java:8

EXPOSE 80

ENTRYPOINT ["java", "-jar", "/api-with-dependencies.jar"]

ENV API_PORT 80

COPY target/api-with-dependencies.jar /
