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

import com.datasqrl.compile.TestPlan;
import com.datasqrl.config.PackageJson;
import com.datasqrl.engine.database.relational.JdbcStatement;
import com.datasqrl.graphql.SqrlObjectMapper;
import com.datasqrl.util.FlinkOperatorStatusChecker;
import com.datasqrl.util.ResultSetPrinter;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;

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

  private static final String GRAPHQL_ENDPOINT = "http://localhost:8888/graphql";
  private static final String SNAPSHOT_EXT = ".snapshot";

  private final Path rootDir;
  private final Path planDir;
  private final PackageJson sqrlConfig;
  private final Configuration flinkConfig;
  private final Map<String, String> env;

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
    // 1. Init DatasqrlRun
    var run = DatasqrlRun.nonBlocking(planDir, sqrlConfig, flinkConfig, env);

    var testConfig = sqrlConfig.getTestConfig();
    // Initialize snapshot directory
    var snapshotDir = testConfig.getSnapshotDir(rootDir);
    // Check if the directory exists, create it if it doesn’t
    if (!Files.exists(snapshotDir)) {
      Files.createDirectories(snapshotDir);
    }
    // Collects all exceptions during the testing
    var exceptions = new ArrayList<Exception>();
    // Retrieve test plan
    TestPlan testPlan = null;
    // Read configuration
    var delaySec = testConfig.getDelaySec();
    var mutationWaitSec = testConfig.getMutationDelaySec();
    var requiredCheckpoints = testConfig.getRequiredCheckpoints();

    // It is possible that no test plan exists, such as no test queries.
    // We still run it for exports or other explicit tests the user created outside the test
    // framework
    var testJson = planDir.resolve("test.json").toFile();
    if (testJson.exists()) {
      testPlan = SqrlObjectMapper.MAPPER.readValue(testJson, TestPlan.class);
    }

    // 2. Run the DataSQRL pipeline
    try {
      var result = run.run();
      Thread.sleep(1000);

      var subscriptionClients = new ArrayList<SubscriptionClient>();
      // 3. Execute subscription & mutation operations against the API and snapshot results
      if (testPlan != null) {
        // 1. add the graphql subscriptions to the test plan
        // 2. connect a websocket to the graphql server hosted in vertx
        // 3. run all the graphql subscriptions requests by sending them to the websocket
        // 4. collect the response data (from the websocket) that the pipeline has written to kafka.

        // Initialize subscriptions
        var subscriptionFutures = new ArrayList<CompletableFuture<Void>>();

        for (var subscription : testPlan.getSubscriptions()) {
          var client =
              new SubscriptionClient(
                  subscription.getName(), subscription.getQuery(), subscription.getHeaders());
          subscriptionClients.add(client);
          var future = client.start();
          subscriptionFutures.add(future);
        }

        // Wait for all subscriptions to be connected
        CompletableFuture.allOf(subscriptionFutures.toArray(new CompletableFuture[0])).join();

        if (!subscriptionClients.isEmpty()) {
          // sqrl server takes a while to create the server side wiring that connects kafka to
          // websocket.  We don't have any mechanism to notify us when the server is ready
          // we should replace this with some sort of GET /status that list which subscriptions are
          // ready
          Thread.sleep(5_000);
        }

        var exList =
            executeAndSnapshotGraphqlQueries(testPlan.getMutations(), snapshotDir, mutationWaitSec);
        exceptions.addAll(exList);
      }

      // 4. Wait for the Flink job to finish or the configured delay
      if (delaySec == -1) {
        var flinkOperatorStatusChecker =
            new FlinkOperatorStatusChecker(
                result.getJobClient().get().getJobID().toString(), requiredCheckpoints);
        flinkOperatorStatusChecker.run();
      } else {
        try {
          for (int i = 0; i < delaySec; i++) {
            // break early if job is done
            try {
              var jobStatusCompletableFuture =
                  result.getJobClient().map(JobClient::getJobStatus).get();
              var status = jobStatusCompletableFuture.get(1, TimeUnit.SECONDS);
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

      // 5. Run the queries against JDBC & API, finish subscriptions and snapshot the results
      if (testPlan != null) {
        var jdbcExList = queryAndSnapshotJdbcViews(testPlan.getJdbcViews(), snapshotDir);
        exceptions.addAll(jdbcExList);

        var gqlExList = executeAndSnapshotGraphqlQueries(testPlan.getQueries(), snapshotDir, 0);
        exceptions.addAll(gqlExList);

        // Stop subscriptions
        for (var client : subscriptionClients) {
          client.stop();
        }

        // Collect messages and write to snapshots
        for (var client : subscriptionClients) {
          var messages = client.getMessages();

          assert messages.size() < 1000 : "Too many messages: %d > 1000".formatted(messages.size());

          var data =
              messages.stream()
                  // to guarantee that snapshots are stable, must sort the responses by json
                  // contents
                  .sorted()
                  .collect(Collectors.joining(",", "[", "]"));

          snapshot(snapshotDir, client.getName(), data, exceptions);
        }

        var expectedSnapshots =
            Stream.of(
                    testPlan.getJdbcViews().stream().map(f -> f.getName() + SNAPSHOT_EXT),
                    collectGraphqlQueryNames(testPlan.getQueries()).stream(),
                    collectGraphqlQueryNames(testPlan.getMutations()).stream(),
                    collectGraphqlQueryNames(testPlan.getSubscriptions()).stream())
                .flatMap(stream -> stream)
                .collect(Collectors.toSet());

        // Check all snapshots in the directory
        try (var directoryStream = Files.newDirectoryStream(snapshotDir, "*" + SNAPSHOT_EXT)) {
          for (var path : directoryStream) {
            var snapshotFileName = path.getFileName().toString();
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

    // 6. Print the test results on the command line
    int exitCode = 0;
    if (!exceptions.isEmpty()) {
      for (var e : exceptions) {
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

  private List<Exception> queryAndSnapshotJdbcViews(
      List<JdbcStatement> jdbcViews, Path snapshotDir) {
    var exceptions = new ArrayList<Exception>();

    var url = env.get("POSTGRES_JDBC_URL");
    var username = env.get("POSTGRES_USERNAME");
    var password = env.get("POSTGRES_PASSWORD");
    try (var conn = DriverManager.getConnection(url, username, password)) {

      for (var view : jdbcViews) {
        var viewName = view.getName();

        try (var stmt = conn.createStatement()) {
          var resultSet = stmt.executeQuery("SELECT * FROM \"%s\" LIMIT 10".formatted(viewName));
          var data = ResultSetPrinter.toString(resultSet);

          snapshot(snapshotDir, viewName, data, exceptions);
        }
      }
    } catch (Exception ex) {
      exceptions.add(ex);
    }

    return exceptions;
  }

  @SneakyThrows
  private List<Exception> executeAndSnapshotGraphqlQueries(
      List<TestPlan.GraphqlQuery> queries, Path snapshotDir, int mutationWait) {
    var exceptions = new ArrayList<Exception>();

    for (var query : queries) {
      // Execute queries
      var data = executeQuery(query.getQuery(), query.getHeaders());

      // Snapshot result
      snapshot(snapshotDir, query.getName(), data, exceptions);

      // Wait before next mutation
      if (mutationWait > 0) {
        Thread.sleep(mutationWait * 1000L);
      }
    }

    return exceptions;
  }

  private List<String> collectGraphqlQueryNames(List<TestPlan.GraphqlQuery> queries) {
    return queries.stream().map(f -> f.getName() + SNAPSHOT_EXT).toList();
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
  private void snapshot(Path snapshotDir, String name, String rawJson, List<Exception> exceptions) {

    var snapshotPath = snapshotDir.resolve(name + SNAPSHOT_EXT);
    var content = format(rawJson);

    // Existing snapshot logic
    if (Files.exists(snapshotPath)) {
      var existingSnapshot = Files.readString(snapshotPath);
      if (existingSnapshot.equals(content)) {
        exceptions.add(new SnapshotOkException(name));
      } else if (format(existingSnapshot).equals(content)) {
        Files.writeString(snapshotPath, content);
        exceptions.add(new SnapshotOkException(name));
      } else {
        exceptions.add(new SnapshotMismatchException(name, existingSnapshot, content));
      }
    } else {
      Files.writeString(snapshotPath, content);
      exceptions.add(new SnapshotCreationException(name));
    }
  }

  @SneakyThrows
  private String format(String rawData) {
    try {
      var data = SqrlObjectMapper.MAPPER.readValue(rawData, Object.class);
      return SqrlObjectMapper.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(data);
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
}
