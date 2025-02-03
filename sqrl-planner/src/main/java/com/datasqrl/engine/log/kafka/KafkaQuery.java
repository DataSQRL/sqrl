package com.datasqrl.engine.log.kafka;

import com.datasqrl.engine.ExecutableQuery;
import com.datasqrl.engine.pipeline.ExecutionStage;
import java.util.List;
import lombok.Value;

@Value
public class KafkaQuery implements ExecutableQuery {

  ExecutionStage stage;
  String topicName;
  List<String> filterColumnNames;

}
