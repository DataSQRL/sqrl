# Use Gradle 8.6 with JDK 11 as the base image
FROM gradle:8.6-jdk11 AS build

# Update the package list and install the 'patch' utility
RUN apt-get update && apt-get install -y maven patch

# Set the working directory inside the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app/

ENV TZ=America/Los_Angeles

# Run Maven clean and install commands, skipping tests
RUN mvn clean install -DskipTests
