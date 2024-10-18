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
  private String JOB_ID; // Set your Flink job ID
  private static final long POLLING_INTERVAL_MS = 1000; // Poll every 1 second
  private int requiredSuccessfulCheckpoints;

  public void run() {
    try {
      boolean conditionsMet = checkIfOperatorsStoppedAndCheckpointsCompleted(JOB_ID);
      if (conditionsMet) {
        System.out.println("Operators have stopped propagating data and required checkpoints have been completed.");
      } else {
        System.out.println("Operators are still processing data or required checkpoints not completed yet.");
      }
    } catch (Exception e) {
      // Can throw if job finishes early (bounded jobs)
    }
  }

  public boolean checkIfOperatorsStoppedAndCheckpointsCompleted(String jobId) throws Exception {
    int threshold = 2; // Number of consecutive idle polls required
    int consecutiveIdlePolls = 0;

    while (true) {
      boolean isIdle = checkIfAllOperatorsIdle(jobId);
      boolean checkpointsCompleted = checkIfRequiredCheckpointsCompleted(jobId);

      if (isIdle && checkpointsCompleted) {
        consecutiveIdlePolls++;
        if (consecutiveIdlePolls >= threshold) {
          return true; // Conditions met
        }
      } else {
        consecutiveIdlePolls = 0; // Reset counter if conditions not met
      }

      // Sleep before the next poll
      Thread.sleep(POLLING_INTERVAL_MS);
    }
  }

  public boolean checkIfAllOperatorsIdle(String jobId) throws Exception {
    String taskMetricsUrl = FLINK_REST_URL + "/jobs/" + jobId; // Get the job details
    String jsonResponse = getResponseFromUrl(taskMetricsUrl);
    // Parse the vertex (operator) metrics
    return areAllVerticesIdle(jsonResponse);
  }

  @SneakyThrows
  public static boolean areAllVerticesIdle(String jsonResponse) {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonObject = mapper.readTree(jsonResponse);
    ArrayNode verticesArray = (ArrayNode) jsonObject.get("vertices");

    for (JsonNode vertexElement : verticesArray) {
      ObjectNode vertex = (ObjectNode) vertexElement;
      if (!isVertexIdle(vertex)) {
        System.out.println("Vertex not idle");
        return false; // If any operator is not idle, return false
      }
    }
    System.out.println("All operators are idle");
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
    return hasMetricsStopped(vertex.get("id").asText(), readRecords, writeRecords);
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
    return currentReadRecords == previous.readRecords && currentWriteRecords == previous.writeRecords;
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

  public boolean checkIfRequiredCheckpointsCompleted(String jobId) throws Exception {
    String checkpointsUrl = FLINK_REST_URL + "/jobs/" + jobId + "/checkpoints"; // Get the checkpoints info
    String jsonResponse = getResponseFromUrl(checkpointsUrl);
    int completedCheckpoints = getCompletedCheckpointsCount(jsonResponse);
    System.out.println("Completed checkpoints: " + completedCheckpoints);
    return completedCheckpoints >= requiredSuccessfulCheckpoints;
  }

  public int getCompletedCheckpointsCount(String jsonResponse) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonObject = mapper.readTree(jsonResponse);
    JsonNode countsNode = jsonObject.get("counts");
    int completedCheckpoints = countsNode.get("completed").asInt();
    return completedCheckpoints;
  }
}
