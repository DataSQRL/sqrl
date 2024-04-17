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
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Map;
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
      description = "Snapshot path")
  protected Path snapshotPath = null;
  @CommandLine.Option(names = {"--tests"},
      description = "Snapshot path")
  protected Path tests = null;

  @SneakyThrows
  @Override
  public void execute(ErrorCollector errors) {
    super.execute(errors, this.profiles, snapshotPath == null ?
        root.rootDir.resolve("snapshots") : snapshotPath,
        tests == null ?
            root.rootDir.resolve("tests") : tests, ExecutionGoal.TEST);

  }

  @SneakyThrows
  @Override
  protected void postprocess(PackageJson sqrlConfig, Packager packager, Path targetDir, PhysicalPlan plan,
      TestPlan right, ErrorCollector errors) {
    super.postprocess(sqrlConfig, packager, targetDir, plan, right, errors);
    Path compose = targetDir.resolve("docker-compose.yml");
    final DockerComposeContainer<?> environment =
        new DockerComposeContainer<>(compose.toFile())
            .withBuild(true);

    Map map = SqrlObjectMapper.YAML_INSTANCE.readValue(compose.toFile(), Map.class);
    Map<String, Object> servicesMap = (Map<String, Object>)map.get("services");

    Logger logger = LoggerFactory.getLogger(TestCommand.class);
    Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(logger);
    servicesMap.keySet().stream()
            .forEach(s->environment.withLogConsumer(s, logConsumer));

    try {
      environment.start();

      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        logger.info("Shutting down, stopping the environment...");
        environment.stop();
      }));

      ContainerState testContainer = environment.getContainerByServiceName("test").get();

      while (testContainer.isRunning()) {
        Thread.sleep(1000);
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

  @Override
  public PackageJson createDefaultConfig(ErrorCollector errors) {
    throw new RuntimeException("package.json required");
  }

  @Override
  public ExecutionGoal getGoal() {
    return ExecutionGoal.TEST;
  }
}
