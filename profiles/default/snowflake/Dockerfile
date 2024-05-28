# Use the official Gradle image with JDK 11
FROM gradle:jdk11 as builder

# Copy the local code to the container
COPY --chown=gradle:gradle . /home/gradle/src

# Set the working directory
WORKDIR /home/gradle/src

# Run the Gradle shadowJar task to build the project with all dependencies into a fat jar
RUN gradle shadowJar --no-daemon

# Use the official OpenJDK 11 image for the runtime environment
FROM openjdk:11-jre-slim

# Copy the fat jar from the build stage
COPY --from=builder /home/gradle/src/build/libs/*.jar /app/application.jar

# Set the working directory for the application
WORKDIR /app

# Command to execute the jar
CMD ["java", "-jar", "application.jar"]