/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.auto.service.AutoService;
import java.io.IOException;
import java.io.Serializable;
import org.apache.kafka.clients.admin.NewTopic;

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
    gen.writeObjectField(
        "numPartitions", newTopic.numPartitions() == -1 ? 1 : newTopic.numPartitions());
    gen.writeObjectField(
        "replicationFactor", newTopic.replicationFactor() == -1 ? 1 : newTopic.replicationFactor());
    gen.writeObjectField("replicasAssignments", newTopic.replicasAssignments());
    gen.writeObjectField("configs", newTopic.configs());
    gen.writeEndObject();
  }
}
