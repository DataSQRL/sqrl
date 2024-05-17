FROM docker.io/bitnami/kafka:3.4.0-debian-11-r38

RUN echo '#!/bin/bash' > /opt/bitnami/scripts/kafka/create-topics.sh \
<#list kafka["topics"] as topic>
    && echo '/opt/bitnami/kafka/bin/kafka-topics.sh --create --bootstrap-server kafka:9092 --topic ${topic.name} --partitions ${topic.numPartitions} --replication-factor ${topic.replicationFactor}' >> /opt/bitnami/scripts/kafka/create-topics.sh \
</#list>
    && chmod +x /opt/bitnami/scripts/kafka/create-topics.sh
