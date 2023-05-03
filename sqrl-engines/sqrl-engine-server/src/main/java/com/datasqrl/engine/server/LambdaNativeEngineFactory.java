package com.datasqrl.engine.server;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.ExecutionEngine;
import com.google.auto.service.AutoService;
import java.nio.file.Path;
import java.time.Duration;
import lombok.NonNull;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.utility.MountableFile;

@AutoService(EngineFactory.class)
public class LambdaNativeEngineFactory extends GraphqlServerEngineFactory {

  public static final String ENGINE_NAME = "aws-lambda-native-arm64";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public ExecutionEngine initialize(@NonNull SqrlConfig config) {
    return new LambdaNativeEngine();
  }

  public static class LambdaNativeEngine extends GenericJavaServerEngine {

    public LambdaNativeEngine() {
      super(ENGINE_NAME);
    }

    @Override
    public void generateAssets(Path buildDir) {

      Path sourcePath = buildDir;
      String targetPath = "/build";
      MountableFile mountableFile = MountableFile.forHostPath(sourcePath);

      GenericContainer<?> container = new GenericContainer<>("engine-aws-lambda-native")
          .withStartupCheckStrategy(new OneShotStartupCheckStrategy()
              .withTimeout(Duration.ofMinutes(10)))
          .withFileSystemBind(mountableFile.getResolvedPath(), targetPath);

      container.start();
      container.stop();
    }
  }
}
