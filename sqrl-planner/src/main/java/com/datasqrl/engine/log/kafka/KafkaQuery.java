package com.datasqrl.engine.log.kafka;

import java.util.Map;

import com.datasqrl.engine.ExecutableQuery;
import com.datasqrl.engine.pipeline.ExecutionStage;

import lombok.Value;

@Value
public class KafkaQuery implements ExecutableQuery {

  ExecutionStage stage;
  String topicName;
  /**
   * The name of the column that we filter on with the index of
   * the argument.
   */
  Map<String,Integer> filterColumnNames;

}
