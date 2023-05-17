package com.datasqrl.graphql.kafka;

import com.datasqrl.graphql.server.SinkResult;
import lombok.Value;
import org.apache.kafka.clients.producer.RecordMetadata;

@Value
public class KafkaSinkResult extends SinkResult {
  RecordMetadata recordMetadata;
}
