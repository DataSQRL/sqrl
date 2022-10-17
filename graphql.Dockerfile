FROM --platform=linux/amd64 eclipse-temurin:11-jdk-alpine
WORKDIR /usr/src/app
COPY sqrl-core/target/sqrl-1.0-SNAPSHOT.jar sqrl-1.0-SNAPSHOT.jar

ENTRYPOINT ["java", "-cp", "sqrl-1.0-SNAPSHOT.jar", "ai.datasqrl.graphql.GraphQLServer"]