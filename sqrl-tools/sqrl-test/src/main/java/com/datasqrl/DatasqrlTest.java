package com.datasqrl;

import com.datasqrl.util.FlinkOperatorStatusChecker;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketClient;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.http.impl.WebSocketClientImpl;
import io.vertx.ext.web.client.WebClient;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.TableResult;

public class DatasqrlTest {

  private final Path basePath;
  private final Path planPath;
  private final Map<String, String> env;
  public String GRAPHQL_ENDPOINT = "http://localhost:8888/graphql";

  public static void main(String[] args) {
    DatasqrlTest test = new DatasqrlTest();
    int returnCode = test.run();
    System.exit(returnCode);
  }

  public DatasqrlTest() {
    this(Path.of(System.getProperty("user.dir")).resolve("build"),
        Path.of(System.getProperty("user.dir")).resolve("build").resolve("plan"),
        System.getenv());
  }

  public DatasqrlTest(Path basePath, Path planPath, Map<String, String> env) {
    this.basePath = basePath;
    this.planPath = planPath;
    this.env = env;
  }

  @SneakyThrows
  public int run() {
    DatasqrlRun run = new DatasqrlRun(planPath, env);
    List<Exception> exceptions = new ArrayList<>();

    try {
      TableResult result = run.run(false);

      ObjectMapper objectMapper = new ObjectMapper();
      //todo add file check

      Thread.sleep(1000);

      Map compilerMap = (Map) run.getPackageJson().get("compiler");
      String snapshotPathString = (String) compilerMap.get("snapshotPath");
      Path snapshotDir = Path.of(snapshotPathString);

      // Check if the directory exists, create it if it doesn’t
      if (!Files.exists(snapshotDir)) {
        Files.createDirectories(snapshotDir);
      }

      //It is possible that no test plan exists, such as no test queries.
      // We will still let exports run, though we may want to replace them with blackhole sinks
      if (Files.exists(planPath.resolve("test.json"))) {

        TestPlan testPlan = objectMapper.readValue(planPath.resolve("test.json").toFile(),
            TestPlan.class);

        // Initialize subscriptions
        List<SubscriptionClient> subscriptionClients = new ArrayList<>();
        List<CompletableFuture<Void>> subscriptionFutures = new ArrayList<>();

        for (GraphqlQuery subscription : testPlan.getSubscriptions()) {
          SubscriptionClient client = new SubscriptionClient(subscription.getName(), subscription.getQuery(),
              "ws://localhost:8888/graphql");
          subscriptionClients.add(client);
          CompletableFuture<Void> future = client.start();
          subscriptionFutures.add(future);
        }

        // Wait for all subscriptions to be connected
        CompletableFuture.allOf(subscriptionFutures.toArray(new CompletableFuture[0])).join();

        // Execute mutations
        for (GraphqlQuery query : testPlan.getMutations()) {
          //Execute queries
          String data = executeQuery(query.getQuery());

          //Snapshot result
          Path snapshotPath = snapshotDir.resolve(query.getName() + ".snapshot");
          snapshot(snapshotPath, query.getName(), data, exceptions);
        }
        CompiledPlan plan = run.startFlink();
        result = plan.execute();
//        if (hold) {
//          execute.print();
//        }
        long delaySec = 30;
        int requiredCheckpoints = 0;
        //todo: fix get package json
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

        outer:while (true) {
          for (SubscriptionClient client : subscriptionClients) {
            if (client.getMessages().isEmpty()) {
              Thread.sleep(2000);
              System.out.println("Sleep");
            } else {
              break outer;
            }
          }
        }

        // Stop subscriptions
        for (SubscriptionClient client : subscriptionClients) {
          client.stop();
        }

        // Collect messages and write to snapshots
        for (SubscriptionClient client : subscriptionClients) {
          List<String> messages = client.getMessages();
          ObjectMapper om = new ObjectMapper();
          String data = om.writeValueAsString(messages);
          Path snapshotPath = snapshotDir.resolve(client.getName() + ".snapshot");
          snapshot(snapshotPath, client.getName(), data, exceptions);
        }

        try {
          JobExecutionResult jobExecutionResult = result.getJobClient().get().getJobExecutionResult()
              .get(2, TimeUnit.SECONDS); //flink will hold if the minicluster is stopped
        } catch (ExecutionException e) {
          //try to catch the job failure if we can
          exceptions.add(new JobFailureException(e));
        } catch (Exception e) {
        }

        // Execute queries
        for (GraphqlQuery query : testPlan.getQueries()) {
          //Execute queries
          String data = executeQuery(query.getQuery());

          //Snapshot result
          Path snapshotPath = snapshotDir.resolve(query.getName() + ".snapshot");
          snapshot(snapshotPath, query.getName(), data, exceptions);
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
        List<String> expectedSnapshots = ListUtils.union(expectedSnapshotsQueries,
            ListUtils.union(expectedSnapshotsMutations, expectedSnapshotsSubscriptions));
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

  // Simplified example WebSocket client code using Vert.x
  public static class SubscriptionClient {
    private final String name;
    private final String query;
    private final List<String> messages = new ArrayList<>();
    private WebSocket webSocket;
    private final Vertx vertx = Vertx.vertx();
    private final CompletableFuture<Void> connectedFuture = new CompletableFuture<>();

    public SubscriptionClient(String name, String query, String endpoint) {
      this.name = name;
      this.query = query;
    }

    public CompletableFuture<Void> start() {
      Future<io.vertx.core.http.WebSocket> connect = vertx.createWebSocketClient()
          .connect(8888, "localhost", "/graphql");

      connect.onSuccess(ws -> {
        this.webSocket = ws;
        System.out.println("WebSocket opened for subscription: " + name);

        // Set a message handler for incoming messages
        ws.textMessageHandler(this::handleTextMessage);

        // Send initial connection message
        sendConnectionInit();
        connectedFuture.complete(null);
      }).onFailure(throwable -> {
        throwable.printStackTrace();
        System.err.println("Failed to open WebSocket for subscription: " + name);
        connectedFuture.completeExceptionally(throwable);
      });

      return connectedFuture;
    }

    private void sendConnectionInit() {
      Map<String, Object> message = Map.of("type", "connection_init");
      sendMessage(message);
    }

    private void sendSubscribe() {
      Map<String, Object> payload = Map.of("query", query);
      Map<String, Object> message = Map.of(
          "id", "1",
          "type", "subscribe",
          "payload", payload
      );
      sendMessage(message);
    }

    private void sendMessage(Map<String, Object> message) {
      try {
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(message);
        System.out.println("Sending: "+ json);
        webSocket.writeTextMessage(json);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    private void handleTextMessage(String data) {
      // Handle the incoming messages
      System.out.println("Data: "+data);
      try {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> message = objectMapper.readValue(data, Map.class);
        String type = (String) message.get("type");

        if ("connection_ack".equals(type)) {
          // Connection acknowledged, send the subscribe message
          sendSubscribe();
        } else if ("next".equals(type)) {
          // Data message received
          Map<String, Object> payload = (Map<String, Object>) message.get("payload");
          Map<String, Object> dataPayload = (Map<String, Object>) payload.get("data");
          String dataStr = objectMapper.writeValueAsString(dataPayload);
          messages.add(dataStr);
        } else if ("complete".equals(type)) {
          // Subscription complete
        } else if ("error".equals(type)) {
          // Handle error
          System.err.println("Error message received: " + data);
          throw new RuntimeException("Error");
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    public void stop() {
      // Send 'complete' message to close the subscription properly
      Map<String, Object> message = Map.of(
          "id", "1",
          "type", "complete"
      );
      sendMessage(message);

      // Close WebSocket
      if (webSocket != null) {
        webSocket.close();
      }
      vertx.close();
    }

    public List<String> getMessages() {
      return messages;
    }

    public String getName() {
      return name;
    }
  }
}
