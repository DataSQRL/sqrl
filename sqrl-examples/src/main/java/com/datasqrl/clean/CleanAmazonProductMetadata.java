package com.datasqrl.clean;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CleanAmazonProductMetadata {
  private static final String DESCRIPTON_FIELD = "description";
  private static final int DESCRIPTION_MIN_LENGTH = 100;
  private static final Set<String> MUST_FIELDS = new HashSet<>(Arrays.asList("asin", "title", DESCRIPTON_FIELD, "brand", "rank"));

  private static final Set<String> COPY_FIELDS = Sets.union(MUST_FIELDS, Set.of("price"));

  public static void main(String[] args) {
    ObjectMapper mapper = new ObjectMapper();
    String folder = "/Users/matthias/Data/datasqrl/examples/amzon/";
    Set<String> titles = new HashSet<>();
    int counter = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(folder + "meta_AMAZON_FASHION.json"));
        BufferedWriter writer = new BufferedWriter(new FileWriter(folder + "amazon_fashion.json"))) {
      String line;
      while ((line = reader.readLine()) != null) {
        JsonNode node = mapper.readTree(line);
        if (MUST_FIELDS.stream().allMatch(node::has) && MUST_FIELDS.stream().noneMatch(f -> node.get(f).isNull())) {
          ObjectNode newNode = mapper.createObjectNode();
          COPY_FIELDS.forEach(field -> {
            if (node.has(field) && !node.get(field).isNull()) newNode.put(field, convert2String(node.get(field)));
          });
          if (newNode.get(DESCRIPTON_FIELD).asText().length()<DESCRIPTION_MIN_LENGTH) continue;
          String title = newNode.get("title").asText();
          if (title.length()>1000) continue; //likely garbage
          if (titles.contains(title.toLowerCase())) continue; //duplicate
          long rank = extractNumber(newNode.get("rank").asText());
          if (rank==0 || rank>500000) continue;
          if (rank<10000) System.out.println(rank + " - " + title);
          titles.add(title.toLowerCase());
          writer.write(newNode.toString());
          writer.newLine();
          counter++;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    System.out.println("Written: " + counter);
  }

  public static String convert2String(JsonNode node) {
    if (node.isArray()) {
      StringBuilder sb = new StringBuilder();
      for (JsonNode element : node) {
        sb.append(element.asText());
      }
      return sb.toString();
    } else return node.asText();
  }

  static long extractNumber(String s) {
    try {
      Pattern p = Pattern.compile("^\\d+(,\\d+)*");
      Matcher m = p.matcher(s);
      if (m.find()) {
        String numberAsString = m.group();
        numberAsString = numberAsString.replace(",", ""); // remove commas
        return Long.parseLong(numberAsString);
      }
    } catch (Exception e) {
      //Do nothing
    }
    return 0; // return 0 if no number is found
  }
}
