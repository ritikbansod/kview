# Multi-stage build for Kview
# Stage 1: Build the Spring Boot executable jar
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -B dependency:go-offline
COPY src ./src
RUN mvn -B -DskipTests clean package

# Stage 2: Minimal runtime image
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/target/kview-*.jar app.jar

ENV KVIEW_BIND=0.0.0.0 \
    KVIEW_DATA_DIR=/app/data \
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092

EXPOSE 8090
VOLUME /app/data

ENTRYPOINT ["java", "-jar", "app.jar"]
