package com.datasqrl;

import com.datasqrl.util.FlinkOperatorStatusChecker;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    this(
        Path.of(System.getProperty("user.dir")).resolve("build"),
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
      // todo add file check

      Thread.sleep(1000);

      Map compilerMap = (Map) run.getPackageJson().get("compiler");
      String snapshotPathString = (String) compilerMap.get("snapshotPath");
      Path snapshotDir = Path.of(snapshotPathString);

      // Check if the directory exists, create it if it doesn’t
      if (!Files.exists(snapshotDir)) {
        Files.createDirectories(snapshotDir);
      }

      // It is possible that no test plan exists, such as no test queries.
      // We will still let exports run, though we may want to replace them with blackhole sinks
      if (Files.exists(planPath.resolve("test.json"))) {

        TestPlan testPlan =
            objectMapper.readValue(planPath.resolve("test.json").toFile(), TestPlan.class);
        for (GraphqlQuery query : testPlan.getMutations()) {
          // Execute queries
          String data = executeQuery(query.getQuery());

          // Snapshot result
          Path snapshotPath = snapshotDir.resolve(query.getName() + ".snapshot");
          snapshot(snapshotPath, query.getName(), data, exceptions);
        }
      }

      long delaySec = 30;
      int requiredCheckpoints = 0;
      // todo: fix get package json
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

        } catch (Exception e) {
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
      } catch (Exception e) {
      }

      if (Files.exists(planPath.resolve("test.json"))) {

        TestPlan testPlan =
            objectMapper.readValue(planPath.resolve("test.json").toFile(), TestPlan.class);
        for (GraphqlQuery query : testPlan.getQueries()) {
          // Execute queries
          String data = executeQuery(query.getQuery());

          // Snapshot result
          Path snapshotPath = snapshotDir.resolve(query.getName() + ".snapshot");
          snapshot(snapshotPath, query.getName(), data, exceptions);
        }

        List<String> expectedSnapshotsQueries =
            testPlan.getQueries().stream()
                .map(f -> f.getName() + ".snapshot")
                .collect(Collectors.toList());
        List<String> expectedSnapshotsMutations =
            testPlan.getMutations().stream()
                .map(f -> f.getName() + ".snapshot")
                .collect(Collectors.toList());
        List<String> expectedSnapshots =
            ListUtils.union(expectedSnapshotsQueries, expectedSnapshotsMutations);
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
      run.stop();
      Thread.sleep(1000); // wait for log lines to clear out
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

    HttpRequest request =
        HttpRequest.newBuilder()
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
  private void snapshot(
      Path snapshotPath, String name, String currentResponse, List<Exception> exceptions) {

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

  // Todo: Unify with other testplan
  @AllArgsConstructor
  @NoArgsConstructor
  @Getter
  public static class TestPlan {

    List<GraphqlQuery> queries;
    List<GraphqlQuery> mutations;
  }

  @AllArgsConstructor
  @NoArgsConstructor
  @Getter
  public static class GraphqlQuery {

    String name;
    String query;
  }
}
