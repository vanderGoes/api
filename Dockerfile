FROM jeanblanchard/java:8

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/api-with-dependencies.jar"]

COPY target/api-with-dependencies.jar /