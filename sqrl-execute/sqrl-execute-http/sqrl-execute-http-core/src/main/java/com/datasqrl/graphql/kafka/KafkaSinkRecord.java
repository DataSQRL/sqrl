package com.datasqrl.graphql.kafka;

import com.datasqrl.graphql.server.SinkRecord;
import lombok.Value;

@Value
public class KafkaSinkRecord extends SinkRecord {
  String object;
}
