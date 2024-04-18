FROM docker.io/bitnami/kafka:3.4.0-debian-11-r38

RUN echo '#!/bin/bash' > /opt/bitnami/scripts/kafka/create-topics.sh \
<#if config["pipeline"]?seq_contains("log")>
<#list plan.get("log").getTopics() as topic>
    && echo '/opt/bitnami/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic ${topic.name} --partitions ${topic.numPartitions} --replication-factor ${topic.replicationFactor}' >> /opt/bitnami/scripts/kafka/create-topics.sh \
</#list>
</#if>
    && echo 'exec /opt/bitnami/scripts/kafka/run.sh "$@"' >> /opt/bitnami/scripts/kafka/create-topics.sh \
    && chmod +x /opt/bitnami/scripts/kafka/create-topics.sh
