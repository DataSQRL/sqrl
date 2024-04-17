<#if config["engines"]["log"]??>
#!/bin/bash

<#list plan.get("log").getTopics() as topic>
/opt/bitnami/kafka/bin/kafka-topics.sh --create \
    --bootstrap-server ${config["engines"]["log"]["properties.bootstrap.servers"]} \
    --topic ${topic.name} \
    --partitions ${topic.numPartitions} \
    --replication-factor ${topic.replicationFactor}
</#list>
</#if>