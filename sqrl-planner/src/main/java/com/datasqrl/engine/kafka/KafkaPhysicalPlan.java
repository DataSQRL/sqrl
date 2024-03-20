package com.datasqrl.engine.kafka;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.serializer.Deserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Value;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@Value
public class KafkaPhysicalPlan implements EnginePhysicalPlan {
  SqrlConfig config;

  List<NewTopic> topics;

}
