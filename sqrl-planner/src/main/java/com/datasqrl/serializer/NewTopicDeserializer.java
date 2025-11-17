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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.auto.service.AutoService;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.admin.NewTopic;

@AutoService(StdDeserializer.class)
public class NewTopicDeserializer<T extends Serializable> extends StdDeserializer<NewTopic> {

  public NewTopicDeserializer() {
    super(NewTopic.class);
  }

  @Override
  public NewTopic deserialize(JsonParser jsonParser, DeserializationContext ctxt)
      throws IOException {
    JsonNode node = jsonParser.getCodec().readTree(jsonParser);
    var name = node.get("name").asText();
    Optional<Integer> numPartitions = Optional.ofNullable(node.get("numPartitions").asInt());
    Optional<Short> replicationFactor =
        Optional.ofNullable((short) node.get("replicationFactor").asInt());

    // Creating Map<Integer, List<Integer>> for replicasAssignments
    Map<Integer, List<Integer>> replicasAssignments = null;
    if (node.has("replicasAssignments")) {
      var node1 = node.get("replicasAssignments");
      var f = node1.fields();

      replicasAssignments = new HashMap<>();
      while (f.hasNext()) {
        var entry = f.next();
        List<Integer> values = new ArrayList<>();
        var nodeIterator = entry.getValue().elements();
        while (nodeIterator.hasNext()) {
          values.add(nodeIterator.next().asInt());
        }
        replicasAssignments.put(Integer.parseInt(entry.getKey()), values);
      }
    }

    // Creating Map<String, String> for configs
    Map<String, String> configs = null;
    if (node.has("configs")) {
      var f = node.get("configs").fields();

      configs = new HashMap<>();
      while (f.hasNext()) {
        var entry = f.next();
        configs.put(entry.getKey(), entry.getValue().asText());
      }
    }

    if (replicasAssignments != null) {
      return new NewTopic(name, replicasAssignments).configs(configs);
    } else {
      return new NewTopic(name, numPartitions, replicationFactor).configs(configs);
    }
  }
}
