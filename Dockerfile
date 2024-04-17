FROM maven:3.9.6-eclipse-temurin-11

# Install patch
RUN apt-get update && apt-get install -y patch

WORKDIR /app

COPY . /app/

RUN mvn clean install -DskipTests