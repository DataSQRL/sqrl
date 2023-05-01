/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.utility.MountableFile;

@Slf4j
public class LocalFlinkStreamEngineImpl extends AbstractFlinkStreamEngine {

  public LocalFlinkStreamEngineImpl(ExecutionEnvironmentFactory execFactory) {
    super(execFactory);
  }

  public FlinkStreamBuilder createJob() {
    return new FlinkStreamBuilder(this,
        execFactory.createEnvironment());
  }

  @Override
  public void generateAssets(Path buildDir) {
    GenericContainer<?> container =
        new GenericContainer<>("engine-flink")
            .withStartupCheckStrategy(new OneShotStartupCheckStrategy()
                .withTimeout(Duration.ofMinutes(10))
            );
    String targetPath = "/build";
    MountableFile mountableFile = MountableFile.forHostPath(buildDir);
    container.withFileSystemBind(mountableFile.getResolvedPath(), targetPath);

    container.start();
    container.stop();

  }
}
