package com.datasqrl.engine.kafka.serializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.auto.service.AutoService;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.codec.binary.Base64;
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
    String name = node.get("name").asText();
    Optional<Integer> numPartitions = Optional.ofNullable(node.get("numPartitions").asInt());
    Optional<Short> replicationFactor = Optional.ofNullable(
        (short) node.get("replicationFactor").asInt());

    // Creating Map<Integer, List<Integer>> for replicasAssignments
    Map<Integer, List<Integer>> replicasAssignments = null;
    if (node.has("replicasAssignments")) {
      JsonNode node1 = node.get("replicasAssignments");
      Iterator<Map.Entry<String, JsonNode>> f = node1.fields();

      replicasAssignments = new HashMap<>();
      while (f.hasNext()) {
        Map.Entry<String, JsonNode> entry = f.next();
        List<Integer> values = new ArrayList<>();
        Iterator<JsonNode> nodeIterator = entry.getValue().elements();
        while (nodeIterator.hasNext()) {
          values.add(nodeIterator.next().asInt());
        }
        replicasAssignments.put(Integer.parseInt(entry.getKey()), values);
      }
    }

    // Creating Map<String, String> for configs
    Map<String, String> configs = null;
    if (node.has("configs")) {
      Iterator<Map.Entry<String, JsonNode>> f = node.get("configs").fields();

      configs = new HashMap<>();
      while (f.hasNext()) {
        Map.Entry<String, JsonNode> entry = f.next();
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
