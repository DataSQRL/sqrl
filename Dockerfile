# Use an official Maven image from the Docker Hub as the base image
FROM maven:3.8.6-jdk-11

# Install patch
RUN apt-get update && apt-get install -y patch

# Set the working directory inside the container
WORKDIR /app

# Now, copy the rest of the source code into the container.
COPY . /app/

# Run Maven install to build the project, skipping tests
RUN mvn clean install -DskipTests