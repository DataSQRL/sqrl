COMPOSE_FILE=${config["enabled-engines"]?map(engine -> engine + ".compose.yml")?join(":")}
AWS_REGION=${r"${AWS_REGION:-us-east-1}"}
AWS_ACCESS_KEY_ID=${r"${AWS_ACCESS_KEY_ID:-myaccesskey}"}
AWS_SECRET_ACCESS_KEY=${r"${AWS_SECRET_ACCESS_KEY:-mysecretkey}"}
SNOWFLAKE_ID=${r"${SNOWFLAKE_ID:-mysnowflakeid}"}
SNOWFLAKE_PASSWORD=${r"${SNOWFLAKE_PASSWORD:-mysnowflakepassword}"}