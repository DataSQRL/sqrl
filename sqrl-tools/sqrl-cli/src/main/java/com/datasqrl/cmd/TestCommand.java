/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import com.datasqrl.DatasqrlRun;
import com.datasqrl.compile.TestPlan;
import com.datasqrl.compile.TestPlan.GraphqlQuery;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.TestRunnerConfiguration;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.Packager;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobID;
import org.apache.flink.table.api.TableResult;
import picocli.CommandLine;

@Slf4j
@CommandLine.Command(name = "test", description = "Tests a SQRL script")
public class TestCommand extends AbstractCompilerCommand {
  @CommandLine.Option(names = {"-s", "--snapshot"},
      description = "Path to snapshots")
  protected Path snapshotPath = null;
  @CommandLine.Option(names = {"--tests"},
      description = "Path to test queries")
  protected Path tests = null;

  public String GRAPHQL_ENDPOINT = "http://localhost:8888/graphql";

  @SneakyThrows
  @Override
  public void execute(ErrorCollector errors) {
    super.execute(errors, this.profiles, snapshotPath == null ?
        root.rootDir.resolve("snapshots") :
            snapshotPath.isAbsolute() ? snapshotPath : root.rootDir.resolve(snapshotPath),
        tests == null ? (Files.isDirectory(root.rootDir.resolve("tests")) ?
            Optional.of(root.rootDir.resolve("tests")) : Optional.empty()) :
            Optional.of((tests.isAbsolute() ? tests : root.rootDir.resolve(tests))),
        ExecutionGoal.TEST);
  }

  @SneakyThrows
  @Override
  protected void postprocess(PackageJson sqrlConfig, Packager packager, Path targetDir, PhysicalPlan plan,
      TestPlan right, ErrorCollector errors) {
    super.postprocess(sqrlConfig, packager, targetDir, plan, right, errors);
    Path planPath = root.rootDir.resolve("build").resolve("plan");
    DatasqrlRun run = new DatasqrlRun(planPath, root.getEnv());
    TableResult result = run.run(false);
    try {

      ObjectMapper objectMapper = new ObjectMapper();
      //todo add file check
      TestPlan testPlan = objectMapper.readValue(planPath.resolve("test.json").toFile(),
          TestPlan.class);

      Thread.sleep(1000);

      for (GraphqlQuery query : testPlan.getMutations()) {
        //Execute queries
        String data = executeQuery(query.getQuery());
      }

      sqrlConfig.getTestConfig().flatMap(TestRunnerConfiguration::getDelaySec)
          .ifPresent(sec -> {
            try {
              Thread.sleep(sec.toMillis());
            } catch (Exception e) {
            }
          });

//     waitForFlinkToStop(result.getJobClient().get().getJobID());

      for (GraphqlQuery query : testPlan.getQueries()) {
        //Execute queries
        String data = executeQuery(query.getQuery());
        //Snapshot result
        snapshot(root.rootDir.resolve("snapshots"), query.getName(), data, errors);
      }
    } finally {
      result.getJobClient().get().cancel();
    }
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
  private void snapshot(Path snapshotDir, String name, String currentResponse,
      ErrorCollector errors) {
    // Check if the directory exists, create it if it doesn’t
    if (!Files.exists(snapshotDir)) {
      Files.createDirectories(snapshotDir);
    }

    Path snapshotPath = snapshotDir.resolve(name + ".snapshot");
    if (Files.exists(snapshotPath)) {
      String existingSnapshot = new String(Files.readAllBytes(snapshotPath), "UTF-8");
      if (!existingSnapshot.equals(currentResponse)) {
        logRed("Snapshot mismatch detected for " + name);
        logRed("Expected:" + existingSnapshot);
        logRed("Found   :" + currentResponse);
        this.root.getStatusHook().onFailure(new RuntimeException("Tests could not complete successfully."),
            errors);
      } else {
        logGreen("Snapshot OK for " + name);
      }
    } else {
      Files.write(snapshotPath, currentResponse.getBytes("UTF-8"));
      logGreen("Snapshot saved for " + name + ". Rerun again to verify.");
    }
  }

  private void logGreen(String line) {
    System.out.println("\u001B[32m" + line + "\u001B[0m");
  }

  private void logRed(String line) {
    System.out.println("\u001B[31m" + line + "\u001B[0m");
  }

  @SneakyThrows
  private List<File> getComposePaths(Path targetDir) {
    return Files.list(targetDir)
        .filter(f->f.getFileName().toString().endsWith("compose.yml"))
        .map(f->f.toFile())
        .collect(Collectors.toList());
  }

  @Override
  public ExecutionGoal getGoal() {
    return ExecutionGoal.TEST;
  }
}
