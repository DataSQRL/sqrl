/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import com.datasqrl.compile.TestPlan;
import com.datasqrl.config.PackageJson;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.Packager;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.Arrays;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@CommandLine.Command(name = "test", description = "Tests a SQRL script")
public class TestCommand extends AbstractCompilerCommand {

  @CommandLine.Option(names = {"-s", "--snapshot"},
      description = "Path to snapshots")
  protected Path snapshotPath = null;
  @CommandLine.Option(names = {"--tests"},
      description = "Path to test queries")
  protected Path tests = null;

  private DockerClient dockerClient;

  @SneakyThrows
  @Override
  public void execute(ErrorCollector errors) {
    DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder().build();
    DockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
        .dockerHost(config.getDockerHost())
        .sslConfig(config.getSSLConfig())
        .maxConnections(100)
        .connectionTimeout(Duration.ofSeconds(30))
        .responseTimeout(Duration.ofSeconds(45))
        .build();

    dockerClient = DockerClientImpl.getInstance(config, httpClient);
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
    executeDockerCompose(targetDir.toFile(), "up", "--build");

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("Shutting down, stopping the environment...");
      executeDockerCompose(targetDir.toFile(), "down");
    }));

    // Monitor the test container
    String testServiceName = "deploy-test-1"; // Replace with actual service name
    Optional<String> containerIdOpt = Optional.empty();

    // Loop until the container starts or a timeout occurs
    long startTime = System.currentTimeMillis();
    long timeout = 60000; // Timeout after 60 seconds
    while(System.currentTimeMillis() - startTime < timeout && !containerIdOpt.isPresent()) {
      containerIdOpt = getContainerIdByName(testServiceName);
      if (!containerIdOpt.isPresent()) {
        // Sleep for a short time to avoid busy waiting
        Thread.sleep(1000);
      }
    }

    if (containerIdOpt.isPresent()) {
      String containerId = containerIdOpt.get();
      InspectContainerResponse inspectResponse;
      do {
        inspectResponse = dockerClient.inspectContainerCmd(containerId).exec();
        Thread.sleep(1000);
      } while (inspectResponse.getState().getRunning());

      if (!inspectResponse.getState().getExitCode().equals(0)) {
        String message = "Test container failed";
        throw new RuntimeException(message);
      }
    } else {
      String message = "Test container could not start";
      throw new RuntimeException(message);
    }

    Path logFilePath = targetDir.resolve("test").resolve("jmeter.log");
    try (RandomAccessFile reader = new RandomAccessFile(logFilePath.toFile(), "r")) {
      String line;
      while (true) {
        line = reader.readLine();
        if (line == null) break;

        if (line.contains("ERROR")) {
          // Print in red
          System.out.println("\u001B[31m" + line + "\u001B[0m");
          if (line.contains("Snapshot mismatch") || line.contains("Snapshot saved")) {
            root.statusHook.onFailure(new RuntimeException(""), errors);
          }
        } else if (line.contains("Snapshot OK")) {
          // Print normally
          System.out.println("\u001B[32m" + line + "\u001B[0m");
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void executeDockerCompose(File dockerComposeDir, String... args) {
    String[] baseCommand = {"docker", "compose"};
    String[] fullCommand = Stream.concat(Arrays.stream(baseCommand), Arrays.stream(args))
        .toArray(String[]::new);
    ProcessBuilder processBuilder = new ProcessBuilder();
    processBuilder.redirectErrorStream(true);
    processBuilder.directory(dockerComposeDir.getAbsoluteFile());
    processBuilder.command(fullCommand);

    try {
      Process process = processBuilder.start();

      new Thread(() -> {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
          String line;
          while ((line = reader.readLine()) != null) {
            System.out.println(line);
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }).start();

    } catch (IOException e) {
      throw new RuntimeException("Failed to execute Docker Compose command", e);
    }
  }

  @SneakyThrows
  private List<File> getComposePaths(Path targetDir) {
    return Files.list(targetDir)
        .filter(f -> f.getFileName().toString().endsWith("compose.yml"))
        .map(f -> f.toFile())
        .collect(Collectors.toList());
  }

  private Optional<String> getContainerIdByName(String name) {
    return dockerClient.listContainersCmd().exec().stream()
        .filter(container -> container.getNames()[0].equals("/" + name))
        .map(container -> container.getId())
        .findFirst();
  }

  @Override
  public ExecutionGoal getGoal() {
    return ExecutionGoal.TEST;
  }
}
