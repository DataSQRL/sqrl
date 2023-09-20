package com.datasqrl.transformation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class ConferenceTransform {

  public static void rewriteData(String inputFilename, String outputFilename) {
    ObjectMapper objectMapper = new ObjectMapper();
    List<String> companies = new ArrayList<>();
    String timestamp = Instant.now().toString();
    try {
      BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilename));
      JsonNode rootNode = objectMapper.readTree(new File(inputFilename));
      if (rootNode.isArray()) {
        for (JsonNode node : rootNode) {
          ObjectNode talk = (ObjectNode)node;
          talk.put("last_updated", timestamp);
          String jsonString = objectMapper.writeValueAsString(talk);
          writer.write(jsonString);
          writer.newLine();
          JsonNode speakers = talk.get("speakers");
          for (JsonNode speaker : speakers) {
            String company = speaker.get("company").asText();
            companies.add(company.trim().toLowerCase());
          }
        }
      }
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    Map<String, Long> counts = companies.stream()
        .collect(Collectors.groupingBy(e -> e, Collectors.counting()));

    Map<String, Long> sortedCounts = new TreeMap<>(counts);

    sortedCounts.forEach((k, v) -> System.out.println(k + ": " + v));
  }

}
