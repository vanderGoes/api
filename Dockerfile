FROM jeanblanchard/java:8

EXPOSE 8080

COPY target/api-with-dependencies.jar /

ENTRYPOINT ["java", "-jar", "/api-with-dependencies.jar"]
