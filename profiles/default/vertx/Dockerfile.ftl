FROM ${config["compile"]["sqrl-vertx-image"]}

COPY server-model.json /opt/sqrl/server-model.json
COPY server-config.json /opt/sqrl/server-config.json
COPY snowflake-config.json /opt/sqrl/snowflake-config.json
