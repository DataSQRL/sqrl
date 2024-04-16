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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
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
            root.rootDir.resolve("tests") : tests);

  }

  @SneakyThrows
  @Override
  protected void postprocess(PackageJson sqrlConfig, Packager packager, Path targetDir, PhysicalPlan plan,
      TestPlan right, ErrorCollector errors) {
    super.postprocess(sqrlConfig, packager, targetDir, plan, right, errors);
    final DockerComposeContainer<?> environment =
        new DockerComposeContainer<>(targetDir.resolve("docker-compose.yml").toFile())
            .withBuild(true);

    Logger logger = LoggerFactory.getLogger(TestCommand.class);
    Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(logger);
    environment.withLogConsumer("flink-job-submitter", logConsumer);
    environment.withLogConsumer("test", logConsumer);
    environment.withLogConsumer("flink-taskmanager", logConsumer);
    environment.withLogConsumer("flink-jobmanager", logConsumer);

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
