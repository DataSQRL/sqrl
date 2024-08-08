package com.datasqrl.engine.log.kafka;

import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.engine.EnginePhysicalPlan;
import java.util.List;
import lombok.Value;

@Value
public class KafkaPhysicalPlan implements EnginePhysicalPlan {

  List<NewTopic> topics;

  @Override
  public Type getType() {
    return Type.LOG;
  }
}
