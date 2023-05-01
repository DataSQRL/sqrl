package com.datasqrl.engine.server;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.ExecutionEngine;
import com.google.auto.service.AutoService;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import lombok.NonNull;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.MountableFile;

@AutoService(EngineFactory.class)
public class VertxEngineFactory extends GraphqlServerEngineFactory {

  public static final String ENGINE_NAME = "vertx";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public ExecutionEngine initialize(@NonNull SqrlConfig config) {
    return new VertxEngine();
  }

  public static class VertxEngine extends GenericJavaServerEngine {

    public VertxEngine() {
      super(ENGINE_NAME);
    }

    @Override
    public void generateAssets(Path buildDir) {

      Path sourcePath = buildDir;
      String targetPath = "/build";
      MountableFile mountableFile = MountableFile.forHostPath(sourcePath);

      GenericContainer<?> container = new GenericContainer<>("engine-vertx")
          .withStartupCheckStrategy(new OneShotStartupCheckStrategy()
              .withTimeout(Duration.ofMinutes(10)))
          .withFileSystemBind(mountableFile.getResolvedPath(), targetPath);

      container.start();
      container.stop();
    }
  }
}
