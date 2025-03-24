package com.datasqrl;

import com.datasqrl.util.FlinkOperatorStatusChecker;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.vertx.core.Vertx;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketConnectOptions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.TableResult;

/**
 * The test runner executes the test against the running DataSQRL pipeline and snapshots the results.
 * Snapshotting means that the results of queries are written to files on the first run and subsequently
 * compared to prior snapshots. A test fails if a snapshot does not yet exist or is not identical to the
 * existing snapshot.
 *
 * The test runner executes as follows:
 * 1. Run the DataSQRL pipeline via {@link DatasqrlRun}
 * 2. Execute mutation queries against the API and snapshot results
 * 3. Wait for the Flink job to finish or the configured delay
 * 4. Run the queries against the API and snapshot the results
 * 5. Print the test results on the command line
 */
public class DatasqrlTest {

  //Number of seconds we wait for Flink job to process all the data
  public static final int DEFAULT_DELAY_SEC = 30;

  private final Path basePath;
  private final Path planPath;
  private final Map<String, String> env;
  public String GRAPHQL_ENDPOINT = "http://localhost:8888/graphql";
  private Map<String, List<String>> subscriptionRecords = new HashMap<>(); // subscription records per subscription name
  private WebSocket webSocket;

  public static void main(String[] args) {
    DatasqrlTest test = new DatasqrlTest();
    int returnCode = test.run();
    System.exit(returnCode);
  }

  public DatasqrlTest() {
    this(Path.of(System.getProperty("user.dir")).resolve("build"),
        Path.of(System.getProperty("user.dir")).resolve("build").resolve("deploy").resolve("plan"),
        System.getenv());
  }

  public DatasqrlTest(Path basePath, Path planPath, Map<String, String> env) {
    this.basePath = basePath;
    this.planPath = planPath;
    this.env = env;
  }

  @SneakyThrows
  public int run() {
    ObjectMapper objectMapper = new ObjectMapper();

    //1. Run the DataSQRL pipeline via {@link DatasqrlRun}
    DatasqrlRun run = new DatasqrlRun(planPath, env);
    Map compilerMap = (Map) run.getPackageJson().get("compiler");
    //Initialize snapshot directory
    String snapshotPathString = (String) compilerMap.get("snapshotPath");
    Path snapshotDir = Path.of(snapshotPathString);
    // Check if the directory exists, create it if it doesn’t
    if (!Files.exists(snapshotDir)) {
      Files.createDirectories(snapshotDir);
    }
    //Collects all exceptions during the testing
    List<Exception> exceptions = new ArrayList<>();
    //Retrieve test plan
    Optional<TestPlan> testPlanOpt = Optional.empty();

    //It is possible that no test plan exists, such as no test queries.
    // We still run it for exports or other explicit tests the user created outside the test framework
    if (Files.exists(planPath.resolve("test.json"))) {
      testPlanOpt = Optional.of(objectMapper.readValue(planPath.resolve("test.json").toFile(),
          TestPlan.class));
    }

    try {
      TableResult result = run.run(false);
      //todo add file check instead of sleeping to make sure pipeline has started
      Thread.sleep(1000);

      //2. Execute subscription & mutation queries against the API and snapshot results
      if (testPlanOpt.isPresent()) {
        //TODO: Etienne run all subscription queries async and collect the results.

        // 1. add the graphql subscriptions to the test plan
        // 2. connect a websocket to the graphql server hosted in vertx
        // 3. run all the graphql subscriptions requests by sending them to the websocket
        // 4. collect the response data (from the websocket) that the pipeline has written to kafka.

        connectWebSocket();
        for (GraphqlQuery subscriptionQuery : testPlanOpt.get().getSubscriptions()) {
          executeSubscriptionQuery(subscriptionQuery.getQuery());
        }

        for (GraphqlQuery mutationQuery : testPlanOpt.get().getMutations()) {
          //Execute mutation queries
          String data = executeQuery(mutationQuery.getQuery());
          //Snapshot result
          Path snapshotPath = snapshotDir.resolve(mutationQuery.getName() + ".snapshot");
          snapshot(snapshotPath, mutationQuery.getName(), data, exceptions);
        }
      }

      //3. Wait for the Flink job to finish or the configured delay
      long delaySec = DEFAULT_DELAY_SEC;
      int requiredCheckpoints = 0;
      //todo: fix inject PackageJson and retrieve through interface
      Object testRunner = run.getPackageJson().get("test-runner");
      if (testRunner instanceof Map) {
        Map testRunnerMap = (Map) testRunner;
        Object o = testRunnerMap.get("delay-sec");
        if (o instanceof Number) {
          delaySec = ((Number) o).longValue();
        }
        Object c = testRunnerMap.get("required-checkpoints");
        if (c instanceof Number) {
          requiredCheckpoints = ((Number) c).intValue();
        }
      }

      if (delaySec == -1) {
        FlinkOperatorStatusChecker flinkOperatorStatusChecker = new FlinkOperatorStatusChecker(
            result.getJobClient().get().getJobID().toString(), requiredCheckpoints);
        flinkOperatorStatusChecker.run();
      } else {
        try {
          for (int i = 0; i < delaySec; i++) {
            //break early if job is done
            try {
              CompletableFuture<JobStatus> jobStatusCompletableFuture = result.getJobClient()
                  .map(JobClient::getJobStatus).get();
              JobStatus status = jobStatusCompletableFuture.get(1, TimeUnit.SECONDS);
              if (status == JobStatus.FAILED) {
                exceptions.add(new JobFailureException());
                break;
              }

              if (status == JobStatus.FINISHED || status == JobStatus.CANCELED) {
                break;
              }

            } catch (Exception e) {
              break;
            }

            Thread.sleep(1000);
          }

        } catch (Exception e) {
        }
      }
      
      try {
        JobExecutionResult jobExecutionResult = result.getJobClient().get().getJobExecutionResult()
            .get(2, TimeUnit.SECONDS); //flink will hold if the minicluster is stopped
      } catch (ExecutionException e) {
        //try to catch the job failure if we can
        exceptions.add(new JobFailureException(e));
      } catch (Exception e) {
      }

      //4. Run the queries against the API, finish subscriptions and snapshot the results
      if (testPlanOpt.isPresent()) {
        TestPlan testPlan = testPlanOpt.get();
        for (GraphqlQuery query : testPlan.getQueries()) {
          //Execute queries
          String data = executeQuery(query.getQuery());

          //Snapshot result
          Path snapshotPath = snapshotDir.resolve(query.getName() + ".snapshot");
          snapshot(snapshotPath, query.getName(), data, exceptions);
        }
        // TODO make sure for the exceptions
        //TODO: Etienne terminate subscriptions, sort all records retrieved, and snapshot the result like we do the queries above
        //add snapshot comparison below for subscriptions

        for (GraphqlQuery subscriptionQuery : testPlan.getSubscriptions()) {
          terminateSubscription(extractSubscriptionId(subscriptionQuery.getQuery()));
          Path snapshotPath = snapshotDir.resolve(subscriptionQuery.getName() + ".snapshot");
          final List<String> records = subscriptionRecords.get(subscriptionQuery.getName());
          // sort the records by their hash for determinism because records in subscriptions can arrive in different order.
          records.sort(Comparator.comparingInt(String::hashCode));
          snapshot(snapshotPath, subscriptionQuery.getName(), String.join("\n", records), exceptions);
        }

        List<String> expectedSnapshotsQueries = testPlan.getQueries().stream()
            .map(f -> f.getName() + ".snapshot")
            .collect(Collectors.toList());
        List<String> expectedSnapshotsMutations = testPlan.getMutations().stream()
            .map(f -> f.getName() + ".snapshot")
            .collect(Collectors.toList());
        List<String> expectedSnapshotsSubscriptions = testPlan.getSubscriptions().stream()
                .map(f -> f.getName() + ".snapshot")
                .collect(Collectors.toList());
        List<String> expectedSnapshots =  new ArrayList<>();
        expectedSnapshots.addAll(expectedSnapshotsQueries);
        expectedSnapshots.addAll(expectedSnapshotsMutations);
        expectedSnapshots.addAll(expectedSnapshotsSubscriptions);
        // Check all snapshots in the directory
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(snapshotDir,
            "*.snapshot")) {
          for (Path path : directoryStream) {
            String snapshotFileName = path.getFileName().toString();
            if (!expectedSnapshots.contains(snapshotFileName)) {
              // Snapshot exists on filesystem but missing in the test results
              exceptions.add(new MissingSnapshotException(snapshotFileName));
            }
          }
        }
      }
    } finally {
      run.stop();
      Thread.sleep(1000); //wait for log lines to clear out
    }

    //5. Print the test results on the command line
    int exitCode = 0;
    if (!exceptions.isEmpty()) {
      for (Exception e : exceptions) {
        if (e instanceof SnapshotMismatchException) {
          SnapshotMismatchException ex = (SnapshotMismatchException) e;
          logRed("Snapshot mismatch for test: " + ex.getTestName());
          logRed("Expected: " + ex.getExpected());
          logRed("Actual  : " + ex.getActual());
          exitCode = 1;
        } else if (e instanceof JobFailureException) {
          JobFailureException ex = (JobFailureException) e;
          logRed("Flink job failed to start.");
          ex.printStackTrace();
          exitCode = 1;
        } else if (e instanceof MissingSnapshotException) {
          MissingSnapshotException ex = (MissingSnapshotException) e;
          logRed("Snapshot on filesystem but not in result: " + ex.getTestName());
          exitCode = 1;
        } else if (e instanceof SnapshotCreationException) {
          SnapshotCreationException ex = (SnapshotCreationException) e;
          logGreen("Snapshot created for test: " + ex.getTestName());
          logGreen("Rerun to verify.");
          exitCode = 1;
        } else if (e instanceof SnapshotOkException) {
          SnapshotOkException ex = (SnapshotOkException) e;
          logGreen("Snapshot OK for " + ex.getTestName());
        } else {
          System.err.println(e.getMessage());
          exitCode = 1;
        }
      }
    }
    return exitCode;
  }

  private String extractSubscriptionId(String subscriptionQuery) throws Exception {
    final ObjectMapper objectMapper = new ObjectMapper();
    final JsonNode jsonNode = objectMapper.readTree(subscriptionQuery);
    return jsonNode.get("id").asText();
  }

  private String extractSubscriptionName(String subscriptionRecord) throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode jsonNode = objectMapper.readTree(subscriptionRecord);

    if (jsonNode.has("payload") && jsonNode.get("payload").has("data")) {
      JsonNode dataNode = jsonNode.get("payload").get("data");
      if (dataNode.fieldNames().hasNext()) {
        return dataNode.fieldNames().next();
      }
    }
    throw new IllegalArgumentException("Invalid subscription record format");
  }

  private void executeSubscriptionQuery(String subscriptionQuery) {
    webSocket.writeTextMessage(subscriptionQuery);
  }

  private void terminateSubscription(String subscriptionId) {
    if (webSocket != null) {
      String terminationMessage = "{\"type\":\"complete\",\"id\":\"" + subscriptionId + "\"}";
      webSocket.writeTextMessage(terminationMessage);
    }
  }

  private void connectWebSocket() {
    Vertx vertx = Vertx.vertx();
    io.vertx.core.http.HttpClient client = vertx.createHttpClient();

    URI uri = URI.create(GRAPHQL_ENDPOINT);
    WebSocketConnectOptions options = new WebSocketConnectOptions()
            .setPort(uri.getPort())
            .setHost(uri.getHost())
            .setURI(uri.getPath());

    client.webSocket(options, wsResult -> {
      if (wsResult.succeeded()) {
        webSocket = wsResult.result();
        String initMessage = "{\"type\":\"connection_init\",\"payload\":{}}";

        webSocket.writeTextMessage(initMessage);
        webSocket.handler(message -> {
            try {
              final String messageText = message.toString();
              final String relatedSubscription = extractSubscriptionName(messageText);
              subscriptionRecords.computeIfAbsent(relatedSubscription, k -> new ArrayList<>()).add(messageText);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
      } else {
        throw new RuntimeException("WebSocket connection failed: " + wsResult.cause().getMessage());
      }
    });
  }


  @SneakyThrows
  private String executeQuery(String query) {
    HttpClient client = HttpClient.newHttpClient();

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(GRAPHQL_ENDPOINT))
        .header("Content-Type", "application/graphql")
        .POST(HttpRequest.BodyPublishers.ofString(query))
        .build();

    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new RuntimeException("Failed to post GraphQL query: " + response.body());
    }

    return response.body();
  }

  @SneakyThrows
  private void snapshot(Path snapshotPath, String name, String currentResponse,
      List<Exception> exceptions) {


    // Existing snapshot logic
    if (Files.exists(snapshotPath)) {
      String existingSnapshot = new String(Files.readAllBytes(snapshotPath), "UTF-8");
      if (!existingSnapshot.equals(currentResponse)) {
        exceptions.add(new SnapshotMismatchException(name, existingSnapshot, currentResponse));
      } else {
        exceptions.add(new SnapshotOkException(name));
      }
    } else {
      Files.write(snapshotPath, currentResponse.getBytes("UTF-8"));
      exceptions.add(new SnapshotCreationException(name));
    }
  }

  private void logGreen(String line) {
    System.out.println("\u001B[32m" + line + "\u001B[0m");
  }

  private void logRed(String line) {
    System.out.println("\u001B[31m" + line + "\u001B[0m");
  }

  //Todo: Unify with other testplan
  @AllArgsConstructor
  @NoArgsConstructor
  @Getter
  public static class TestPlan {

    List<GraphqlQuery> queries;
    List<GraphqlQuery> mutations;
    List<GraphqlQuery> subscriptions;
  }

  @AllArgsConstructor
  @NoArgsConstructor
  @Getter
  public static class GraphqlQuery {

    String name;
    String query;
  }
}
