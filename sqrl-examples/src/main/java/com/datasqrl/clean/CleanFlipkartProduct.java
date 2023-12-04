package com.datasqrl.clean;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class CleanFlipkartProduct {

  private static final Set<String> MUST_FIELDS = new HashSet<>(
      Arrays.asList("_id", "title", "description", "brand", "sub_category", "average_rating"));


  public static void main(String[] args) {
    ObjectMapper mapper = new ObjectMapper();
    String folder = "/Users/matthias/Data/datasqrl/examples/flipkart/";
    int counter = 0;
    Set<String> titles = new HashSet<>();
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(folder + "flipkart_fashion.json"))) {
      // Read the JSON array from the file
      ArrayNode arrayNode = (ArrayNode) mapper.readTree(
          new File(folder + "flipkart_fashion_products_dataset.json"));

      // Iterate over the individual JSON objects in the array
      for (JsonNode node : arrayNode) {
        if (MUST_FIELDS.stream().allMatch(node::has) && MUST_FIELDS.stream()
            .noneMatch(f -> node.get(f).asText().isBlank())) {
          ObjectNode newNode = mapper.createObjectNode();
          MUST_FIELDS.forEach(field -> {
            if (node.has(field) && !node.get(field).isNull()) {
              String newField = field;
              if (field.equalsIgnoreCase("_id")) newField = "id";
              newNode.put(newField, node.get(field).asText());
            }
          });
          String title = node.get("title").asText().toLowerCase().trim();
          if (titles.contains(title)) continue;
          titles.add(title);

          writer.write(newNode.toString());
          writer.newLine();
          counter++;
        }
      }
      System.out.println("Records: " + counter);
    } catch(IOException e) {
      e.printStackTrace();
    }
  }
}