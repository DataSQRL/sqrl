package com.datasqrl.engine.log.kafka.serializer;

import java.io.IOException;
import java.io.Serializable;

import org.apache.kafka.clients.admin.NewTopic;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.auto.service.AutoService;

@AutoService(StdSerializer.class)
public class NewTopicSerializer<T extends Serializable> extends StdSerializer<NewTopic> {

  public NewTopicSerializer() {
    super(NewTopic.class);
  }

  @Override
  public void serialize(NewTopic newTopic, JsonGenerator gen, SerializerProvider provider)
      throws IOException {
    gen.writeStartObject();
    gen.writeStringField("name", newTopic.name());
    gen.writeObjectField("numPartitions", newTopic.numPartitions() == -1 ? 1 : newTopic.numPartitions());
    gen.writeObjectField("replicationFactor", newTopic.replicationFactor() == -1 ? 1 : newTopic.replicationFactor());
    gen.writeObjectField("replicasAssignments", newTopic.replicasAssignments());
    gen.writeObjectField("configs", newTopic.configs());
    gen.writeEndObject();
  }
}