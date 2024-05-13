COMPOSE_FILE=${config["enabled-engines"]?map(engine -> engine + ".compose.yml")?join(":")}
