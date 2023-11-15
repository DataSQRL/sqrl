package com.datasqrl.engine.server;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.serializer.Deserializer;
import java.io.IOException;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NoopServerPhysicalPlan implements EnginePhysicalPlan {

  @Override
  public void writeTo(Path deployDir, String stageName, Deserializer serializer)
      throws IOException {
    log.warn("Skipping server physical plan asset generation, no API specified");
  }
}
