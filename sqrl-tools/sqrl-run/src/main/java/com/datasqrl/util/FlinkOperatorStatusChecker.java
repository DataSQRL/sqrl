package com.datasqrl.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

@AllArgsConstructor
public class FlinkOperatorStatusChecker {

  private static final String FLINK_REST_URL = "http://localhost:8081"; // Adjust to your Flink REST URL
  private String JOB_ID = "<your-deployment-id-here>"; // Set your Flink job ID
  private static final long POLLING_INTERVAL_MS = 1000; // Poll every 1 seconds

  public void run() {
    try {
      boolean operatorsStopped = checkIfOperatorsStoppedPropagating(JOB_ID);
      if (operatorsStopped) {
        System.out.println("Operators have stopped propagating data.");
      } else {
        System.out.println("Operators are still processing data.");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public boolean checkIfOperatorsStoppedPropagating(String jobId) throws Exception {
    // You can define a threshold for the number of consecutive polls where metrics don't change
    int threshold = 2;
    int consecutiveIdlePolls = 0;

    while (true) {
      boolean isIdle = checkIfAllOperatorsIdle(jobId);

      if (isIdle) {
        consecutiveIdlePolls++;
        if (consecutiveIdlePolls >= threshold) {
          return true; // The operators have stopped propagating data
        }
      } else {
        consecutiveIdlePolls = 0; // Reset counter if operators are still processing data
      }

      // Sleep before the next poll
      Thread.sleep(POLLING_INTERVAL_MS);
    }
  }

  public boolean checkIfAllOperatorsIdle(String jobId) throws Exception {
    String taskMetricsUrl = FLINK_REST_URL + "/jobs/" + jobId; // Get the job details
    try {
      String jsonResponse = getResponseFromUrl(taskMetricsUrl);
      // Parse the vertex (operator) metrics
      return areAllVerticesIdle(jsonResponse);
    } catch (Exception e) {
      //connection can close if standalone job finishes
      return true;
    }

  }

  @SneakyThrows
  public static boolean areAllVerticesIdle(String jsonResponse) {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonObject = mapper.readTree(jsonResponse);
    ArrayNode verticesArray = (ArrayNode) jsonObject.get("vertices");

    for (JsonNode vertexElement : verticesArray) {
      ObjectNode vertex = (ObjectNode) vertexElement;
      if (!isVertexIdle(vertex)) {
        System.out.println("vertex not idle");
        return false; // If any operator is not idle, return false
      }
    }
    System.out.println("all finished");
    return true; // All operators are idle
  }


  public static boolean isVertexIdle(ObjectNode vertex) {
    ObjectNode metrics = (ObjectNode) vertex.get("metrics");

    long readRecords = metrics.get("read-records").longValue();
    long writeRecords = metrics.get("write-records").longValue();
//        long idleTime = metrics.get("accumulated-idle-time").longValue();
//        long busyTime = metrics.get("accumulated-busy-time").longValue();

    System.out.println(
        "Vertex: " + vertex.get("name").asText() + ", Read: " + readRecords + ", Write: "
            + writeRecords);

    // Check if both read and write records have stopped increasing
    return hasMetricsStopped(vertex.get("id").toString(), readRecords, writeRecords);
  }

  // Keeps track of the previous values of the operator metrics
  private static final java.util.Map<String, OperatorMetrics> previousMetrics = new java.util.HashMap<>();

  public static boolean hasMetricsStopped(String vertexId, long currentReadRecords,
      long currentWriteRecords) {
    OperatorMetrics previous = previousMetrics.getOrDefault(vertexId, new OperatorMetrics(-1, -1));
    previousMetrics.put(vertexId, new OperatorMetrics(currentReadRecords, currentWriteRecords));

    System.out.printf("%s %d %d %d %d%n", vertexId, currentReadRecords, previous.readRecords,
        currentWriteRecords, previous.writeRecords);
    // If the read or write records haven't changed, we consider the operator idle
    return currentReadRecords
        == previous.readRecords && currentWriteRecords == previous.writeRecords;
  }


  // Data class to store previous operator metrics
  static class OperatorMetrics {

    long readRecords;
    long writeRecords;

    OperatorMetrics(long readRecords, long writeRecords) {
      this.readRecords = readRecords;
      this.writeRecords = writeRecords;
    }
  }

  public String getResponseFromUrl(String urlString) throws Exception {
    URL url = new URL(urlString);
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod("GET");
    int status = con.getResponseCode();

    if (status == 200) {
      BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
      String inputLine;
      StringBuilder content = new StringBuilder();
      while ((inputLine = in.readLine()) != null) {
        content.append(inputLine);
      }
      in.close();
      con.disconnect();
      return content.toString();
    } else {
      throw new RuntimeException("Failed to get metrics, HTTP response code: " + status);
    }
  }
}
