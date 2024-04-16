package com.datasqrl.engine.log.kafka;

import com.datasqrl.engine.EnginePhysicalPlan;
import java.util.List;
import java.util.Map;
import lombok.Value;

@Value
public class KafkaPhysicalPlan implements EnginePhysicalPlan {

  List<NewTopic> topics;
}
