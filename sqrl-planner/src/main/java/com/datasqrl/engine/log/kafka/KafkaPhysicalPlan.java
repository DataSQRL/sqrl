package com.datasqrl.engine.log.kafka;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EnginePhysicalPlan;
import lombok.Value;

import java.util.List;

@Value
public class KafkaPhysicalPlan implements EnginePhysicalPlan {
  SqrlConfig config;

  List<NewTopic> topics;

}
