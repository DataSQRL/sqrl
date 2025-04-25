ARG BASE_TAG
FROM datasqrl/base-sqrl:${BASE_TAG}

COPY sqrl-cli/target/sqrl-cli.jar /opt/sqrl/sqrl-cli.jar
COPY sqrl-run/target/sqrl-run.jar /opt/sqrl/sqrl-run.jar
COPY sqrl-test/target/sqrl-test.jar /opt/sqrl/sqrl-test.jar

COPY dockerrun.sh /opt/sqrl/dockerrun.sh

ENTRYPOINT ["/opt/sqrl/dockerrun.sh"]
