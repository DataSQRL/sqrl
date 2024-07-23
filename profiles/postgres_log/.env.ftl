COMPOSE_FILE=${config["enabled-engines"]?map(engine -> engine + ".compose.yml")?join(":")}
POSTGRES_LOG_JDBC_URL=jdbc:postgresql://postgres_log:5432/datasqrl
WAIT_HOSTS=postgres_log:5432