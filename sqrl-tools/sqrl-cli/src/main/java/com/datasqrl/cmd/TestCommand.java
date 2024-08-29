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
import com.datasqrl.util.SqrlObjectMapper;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.BuildImageResultCallback;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.core.command.LogContainerResultCallback;
import com.github.dockerjava.core.command.PullImageResultCallback;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
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
    executeDockerCompose(targetDir.toFile());
    //    List<File> dockerComposePaths = getComposePaths(targetDir);
//    if (dockerComposePaths.isEmpty()) {
//      throw new RuntimeException("Could not find docker compose containers");
//    }
//
//    Logger logger = LoggerFactory.getLogger(TestCommand.class);
//
//    // Start Docker containers
//    for (File dockerComposePath : dockerComposePaths) {
//      Map<String, Object> services = SqrlObjectMapper.YAML_INSTANCE.readValue(dockerComposePath, Map.class);
//      Map<String, Object> servicesMap = (Map<String, Object>) services.get("services");
//
//      for (Map.Entry<String, Object> serviceEntry : servicesMap.entrySet()) {
//        String serviceName = serviceEntry.getKey();
//        Map<String, Object> serviceDetails = (Map<String, Object>) serviceEntry.getValue();
//
//        Map build = (Map) serviceDetails.get("build");
//
//        BuildImageResultCallback dockerfile = dockerClient.buildImageCmd(
//                new File((String) build.get("dockerfile"))).
//            .exec(new BuildImageResultCallback()).awaitCompletion();
//
//        CreateContainerResponse containerResponse = dockerClient
//            .createContainerCmd(dockerfile.awaitImageId())
//            .con
//            .withName(serviceName)
//            .exec();
//
//        dockerClient.startContainerCmd(containerResponse.getId()).exec();
//
//        dockerClient.logContainerCmd(containerResponse.getId())
//            .withFollowStream(true)
//            .withStdOut(true)
//            .withStdErr(true)
//            .exec(new LogContainerResultCallback() {
//              @Override
//              public void onNext(Frame item) {
//                logger.info(new String(item.getPayload()));
//                super.onNext(item);
//              }
//            });
//      }
//    }

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {

//      log.info("Shutting down, stopping the environment...");
      stopAllContainers();
    }));

    // Monitor the test container
    String testServiceName = "test_service_name"; // Replace with actual service name
    Optional<String> containerIdOpt = getContainerIdByName(testServiceName);

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

  private void executeDockerCompose(File dockerComposeDir) {
    ProcessBuilder processBuilder = new ProcessBuilder();
    processBuilder.redirectErrorStream(true);
    processBuilder.directory(dockerComposeDir.getAbsoluteFile());
    processBuilder.command("docker", "compose", "up", "--build");

    try {
      Process process = processBuilder.start();

      try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
//          log.info(line);
          System.out.println(line);
        }
      }

      int exitCode = process.waitFor();
      if (exitCode != 0) {
        throw new RuntimeException("Docker Compose command failed with exit code " + exitCode);
      }

    } catch (IOException | InterruptedException e) {
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

  private void stopAllContainers() {
    dockerClient.listContainersCmd().exec().forEach(container ->
        dockerClient.stopContainerCmd(container.getId()).exec());
  }

  @Override
  public ExecutionGoal getGoal() {
    return ExecutionGoal.TEST;
  }
}
