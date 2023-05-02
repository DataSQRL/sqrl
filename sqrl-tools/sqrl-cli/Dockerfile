FROM eclipse-temurin:11
WORKDIR /usr/src/app
COPY target/sqrl-cli.jar /usr/src/app/sqrl-cli.jar
COPY dockerrun.sh /usr/src/app/dockerrun.sh
ENTRYPOINT ["/usr/src/app/dockerrun.sh"]