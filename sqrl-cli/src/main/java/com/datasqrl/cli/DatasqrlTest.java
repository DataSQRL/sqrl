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
package com.datasqrl.cli;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.datasqrl.config.PackageJson;
import com.datasqrl.util.FlinkOperatorStatusChecker;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.TableResult;

/**
 * The test runner executes the test against the running DataSQRL pipeline and snapshots the
 * results. Snapshotting means that the results of queries are written to files on the first run and
 * subsequently compared to prior snapshots. A test fails if a snapshot does not yet exist or is not
 * identical to the existing snapshot.
 *
 * <p>The test runner executes as follows: 1. Run the DataSQRL pipeline via {@link DatasqrlRun} 2.
 * Execute mutation queries against the API and snapshot results 3. Wait for the Flink job to finish
 * or the configured delay 4. Run the queries against the API and snapshot the results 5. Print the
 * test results on the command line
 */
public class DatasqrlTest {

  private final Path rootDir;
  private final Path planDir;
  private final PackageJson sqrlConfig;
  private final Configuration flinkConfig;
  private final Map<String, String> env;
  public String GRAPHQL_ENDPOINT = "http://localhost:8888/graphql";
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final ObjectWriter jsonWriter = objectMapper.writerWithDefaultPrettyPrinter();

  public DatasqrlTest(
      Path rootDir,
      Path planDir,
      PackageJson sqrlConfig,
      Configuration flinkConfig,
      Map<String, String> env) {
    this.rootDir = rootDir;
    this.planDir = planDir;
    this.sqrlConfig = sqrlConfig;
    this.flinkConfig = flinkConfig;
    this.env = env;
  }

  @SneakyThrows
  public int run() {
    // 1. Run the DataSQRL pipeline via {@link DatasqrlRun}
    DatasqrlRun run = new DatasqrlRun(planDir, sqrlConfig, flinkConfig, env);

    var testConfig = sqrlConfig.getTestConfig();
    // Initialize snapshot directory
    Path snapshotDir = testConfig.getSnapshotDir(rootDir);
    // Check if the directory exists, create it if it doesn’t
    if (!Files.exists(snapshotDir)) {
      Files.createDirectories(snapshotDir);
    }
    // Collects all exceptions during the testing
    List<Exception> exceptions = new ArrayList<>();
    // Retrieve test plan
    Optional<TestPlan> testPlanOpt = Optional.empty();
    // Read configuration
    int delaySec = testConfig.getDelaySec();
    int mutationWait = testConfig.getMutationDelaySec();
    int requiredCheckpoints = testConfig.getRequiredCheckpoints();

    // It is possible that no test plan exists, such as no test queries.
    // We still run it for exports or other explicit tests the user created outside the test
    // framework
    if (Files.exists(planDir.resolve("test.json"))) {
      testPlanOpt =
          Optional.of(
              objectMapper.readValue(planDir.resolve("test.json").toFile(), TestPlan.class));
    }

    try {
      TableResult result = run.run(false, false);
      // todo add file check instead of sleeping to make sure pipeline has started
      Thread.sleep(1000);

      List<SubscriptionClient> subscriptionClients = List.of();
      // 2. Execute subscription & mutation queries against the API and snapshot results
      if (testPlanOpt.isPresent()) {
        TestPlan testPlan = testPlanOpt.get();
        // TODO: Etienne run all subscription queries async and collect the results.

        // 1. add the graphql subscriptions to the test plan
        // 2. connect a websocket to the graphql server hosted in vertx
        // 3. run all the graphql subscriptions requests by sending them to the websocket
        // 4. collect the response data (from the websocket) that the pipeline has written to kafka.

        // Initialize subscriptions
        subscriptionClients = new ArrayList<>();
        List<CompletableFuture<Void>> subscriptionFutures = new ArrayList<>();

        for (GraphqlQuery subscription : testPlan.getSubscriptions()) {
          SubscriptionClient client =
              new SubscriptionClient(
                  subscription.getName(),
                  subscription.getQuery(),
                  subscription.getHeaders() != null ? subscription.getHeaders() : Map.of());
          subscriptionClients.add(client);
          CompletableFuture<Void> future = client.start();
          subscriptionFutures.add(future);
        }

        // Wait for all subscriptions to be connected
        CompletableFuture.allOf(subscriptionFutures.toArray(new CompletableFuture[0])).join();

        if (!subscriptionClients.isEmpty()) {
          // sqrl server takes a while to create the server side wiring that connnects kafka to
          // websocket.  We don't have any mechanism to notify us when the server is ready
          // we should replace this with some sort of GET /status that list which subscriptions are
          // ready
          Thread.sleep(5_000);
        }

        // Execute mutations
        for (GraphqlQuery mutationQuery : testPlan.getMutations()) {
          // Execute mutation queries
          String data =
              executeQuery(
                  mutationQuery.getQuery(),
                  mutationQuery.getHeaders() != null ? mutationQuery.getHeaders() : Map.of());
          // Snapshot result
          Path snapshotPath = snapshotDir.resolve(mutationQuery.getName() + ".snapshot");
          snapshot(snapshotPath, mutationQuery.getName(), data, exceptions);
          // wait before next mutation
          if (mutationWait > 0) Thread.sleep(mutationWait * 1000);
        }
      }

      // 3. Wait for the Flink job to finish or the configured delay
      if (delaySec == -1) {
        FlinkOperatorStatusChecker flinkOperatorStatusChecker =
            new FlinkOperatorStatusChecker(
                result.getJobClient().get().getJobID().toString(), requiredCheckpoints);
        flinkOperatorStatusChecker.run();
      } else {
        try {
          for (int i = 0; i < delaySec; i++) {
            // break early if job is done
            try {
              CompletableFuture<JobStatus> jobStatusCompletableFuture =
                  result.getJobClient().map(JobClient::getJobStatus).get();
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

        } catch (Exception ignored) {
        }
      }

      try {
        JobExecutionResult jobExecutionResult =
            result
                .getJobClient()
                .get()
                .getJobExecutionResult()
                .get(2, TimeUnit.SECONDS); // flink will hold if the minicluster is stopped
      } catch (ExecutionException e) {
        // try to catch the job failure if we can
        exceptions.add(new JobFailureException(e));
      } catch (Exception ignored) {
      }

      // 4. Run the queries against the API, finish subscriptions and snapshot the results
      if (testPlanOpt.isPresent()) {
        TestPlan testPlan = testPlanOpt.get();
        for (GraphqlQuery query : testPlan.getQueries()) {
          // Execute queries
          String data =
              executeQuery(
                  query.getQuery(), query.getHeaders() != null ? query.getHeaders() : Map.of());

          // Snapshot result
          Path snapshotPath = snapshotDir.resolve(query.getName() + ".snapshot");
          snapshot(snapshotPath, query.getName(), data, exceptions);
        }
        // TODO make sure for the exceptions
        // TODO: Etienne terminate subscriptions, sort all records retrieved, and snapshot the
        // result like we do the queries above
        // add snapshot comparison below for subscriptions
        // Stop subscriptions
        for (SubscriptionClient client : subscriptionClients) {
          client.stop();
        }

        // Collect messages and write to snapshots
        for (SubscriptionClient client : subscriptionClients) {
          List<String> messages = client.getMessages();

          assert messages.size() < 1000
              : "Too many messages, that is unexpeccted " + messages.size();

          String data =
              messages.stream()
                  // to guarantee that snapshots are stable, must sort the responses by json
                  // contents
                  .sorted()
                  .collect(Collectors.joining(",", "[", "]"));
          Path snapshotPath = snapshotDir.resolve(client.getName() + ".snapshot");
          snapshot(snapshotPath, client.getName(), data, exceptions);
        }

        List<String> expectedSnapshotsQueries =
            testPlan.getQueries().stream().map(f -> f.getName() + ".snapshot").toList();
        List<String> expectedSnapshotsMutations =
            testPlan.getMutations().stream().map(f -> f.getName() + ".snapshot").toList();
        List<String> expectedSnapshotsSubscriptions =
            testPlan.getSubscriptions().stream().map(f -> f.getName() + ".snapshot").toList();
        List<String> expectedSnapshots = new ArrayList<>();
        expectedSnapshots.addAll(expectedSnapshotsQueries);
        expectedSnapshots.addAll(expectedSnapshotsMutations);
        expectedSnapshots.addAll(expectedSnapshotsSubscriptions);
        // Check all snapshots in the directory
        try (DirectoryStream<Path> directoryStream =
            Files.newDirectoryStream(snapshotDir, "*.snapshot")) {
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
      run.cancel();
      Thread.sleep(1000); // wait for log lines to clear out
    }

    // 5. Print the test results on the command line
    int exitCode = 0;
    if (!exceptions.isEmpty()) {
      for (Exception e : exceptions) {
        if (e instanceof SnapshotMismatchException ex) {
          logRed("Snapshot mismatch for test: " + ex.getTestName());
          logRed("Expected: " + ex.getExpected());
          logRed("Actual  : " + ex.getActual());
          exitCode = 1;
        } else if (e instanceof JobFailureException ex) {
          logRed("Flink job failed to start.");
          ex.printStackTrace();
          exitCode = 1;
        } else if (e instanceof MissingSnapshotException ex) {
          logRed("Snapshot on filesystem but not in result: " + ex.getTestName());
          exitCode = 1;
        } else if (e instanceof SnapshotCreationException ex) {
          logGreen("Snapshot created for test: " + ex.getTestName());
          logGreen("Rerun to verify.");
          exitCode = 1;
        } else if (e instanceof SnapshotOkException ex) {
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
  private String executeQuery(String query, Map<String, String> headers) {
    var client = HttpClient.newHttpClient();

    var requestBuilder =
        HttpRequest.newBuilder()
            .uri(URI.create(GRAPHQL_ENDPOINT))
            .header("Content-Type", "application/graphql")
            .POST(HttpRequest.BodyPublishers.ofString(query));

    if (headers != null) {
      headers.forEach(requestBuilder::header);
    }

    var response = client.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());

    return response.body();
  }

  @SneakyThrows
  private void snapshot(
      Path snapshotPath, String name, String rawJson, List<Exception> exceptions) {

    var content = format(rawJson);

    // Existing snapshot logic
    if (Files.exists(snapshotPath)) {
      var existingSnapshot = Files.readString(snapshotPath, UTF_8);
      if (existingSnapshot.equals(content)) {
        exceptions.add(new SnapshotOkException(name));
      } else if (format(existingSnapshot).equals(content)) {
        Files.writeString(snapshotPath, content, UTF_8);
        exceptions.add(new SnapshotOkException(name));
      } else {
        exceptions.add(new SnapshotMismatchException(name, existingSnapshot, content));
      }
    } else {
      Files.writeString(snapshotPath, content, UTF_8);
      exceptions.add(new SnapshotCreationException(name));
    }
  }

  @SneakyThrows
  private String format(String rawData) {
    try {
      var data = objectMapper.readValue(rawData, Object.class);
      return jsonWriter.writeValueAsString(data);
    } catch (JsonProcessingException e) {
      return rawData;
    }
  }

  private void logGreen(String line) {
    System.out.println("\u001B[32m" + line + "\u001B[0m");
  }

  private void logRed(String line) {
    System.out.println("\u001B[31m" + line + "\u001B[0m");
  }

  // Todo: Unify with other testplan
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
    Map<String, String> headers;
  }
}
