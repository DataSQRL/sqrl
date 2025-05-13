ARG BASE_TAG
FROM datasqrl/base-sqrl:${BASE_TAG}

COPY sqrl-cli/target/sqrl-cli.jar /opt/sqrl/sqrl-cli.jar
COPY sqrl-run/target/sqrl-run.jar /opt/sqrl/sqrl-run.jar
COPY sqrl-test/target/sqrl-test.jar /opt/sqrl/sqrl-test.jar

COPY dockerrun.sh /opt/sqrl/dockerrun.sh
COPY redpanda.yaml /etc/redpanda/redpanda.yaml

VOLUME /data/redpanda

env JAVA_TOOL_OPTIONS="--add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.math=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"

ENTRYPOINT ["/opt/sqrl/dockerrun.sh"]
