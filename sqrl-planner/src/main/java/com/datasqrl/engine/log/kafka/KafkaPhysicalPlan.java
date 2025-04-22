package com.datasqrl.engine.log.kafka;

import java.util.List;

import com.datasqrl.engine.EnginePhysicalPlan;

import lombok.Value;

@Value
public class KafkaPhysicalPlan implements EnginePhysicalPlan {

  List<NewTopic> topics;

}
