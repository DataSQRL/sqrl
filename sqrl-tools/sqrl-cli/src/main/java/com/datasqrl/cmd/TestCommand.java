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
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import picocli.CommandLine;

@CommandLine.Command(name = "test", description = "Tests a SQRL script")
public class TestCommand extends AbstractCompilerCommand {
  @CommandLine.Option(names = {"-s", "--snapshot"},
      description = "Path to snapshots")
  protected Path snapshotPath = null;
  @CommandLine.Option(names = {"--tests"},
      description = "Path to test queries")
  protected Path tests = null;

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
    List<File> dockerComposePaths = getComposePaths(targetDir);
    if (dockerComposePaths.isEmpty()) {
      throw new RuntimeException("Could not find docker compose containers");
    }

    final DockerComposeContainer<?> environment =
        new DockerComposeContainer<>(dockerComposePaths)
            .withBuild(true);

    List<String> services = dockerComposePaths.stream()
        .map(f -> {
          try {
            return SqrlObjectMapper.YAML_INSTANCE.readValue(f, Map.class).get("services");
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        })
        .flatMap(f -> ((Map<String, Object>) f).keySet().stream())
        .collect(Collectors.toList());

    Logger logger = LoggerFactory.getLogger(TestCommand.class);
    Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(logger);
    services.forEach(s->environment.withLogConsumer(s, logConsumer));

    try {
      environment.start();

      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        logger.info("Shutting down, stopping the environment...");
        environment.stop();
      }));

      Optional<ContainerState> containerByServiceName = environment.getContainerByServiceName(
          EngineKeys.TEST);
      if (containerByServiceName.isPresent()) {
        ContainerState testContainer = containerByServiceName.get();
        while (testContainer.isRunning()) {
          Thread.sleep(1000);
        }
      } else {
        String message = "Test container could not start";
        throw new RuntimeException(message);
      }

    } finally {
      environment.stop();
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
        } else if (line.contains("Snapshot OK")){
          // Print normally
          System.out.println("\u001B[32m" + line + "\u001B[0m");
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
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
