FROM maven:3-eclipse-temurin-11 AS build
WORKDIR /usr/src/app
COPY . .
RUN mvn -B -U -T 6 -e clean install --no-transfer-progress -DskipTests=true

FROM eclipse-temurin:11
WORKDIR /usr/src/app
COPY --from=build /usr/src/app/sqrl-tools/sqrl-cli/target/sqrl-cli-0.1-SNAPSHOT-shaded.jar /usr/src/app/sqrl-cli.jar
COPY sqrl-tools/sqrl-cli/dockerrun.sh /usr/src/app/dockerrun.sh
ENTRYPOINT ["/usr/src/app/dockerrun.sh"]