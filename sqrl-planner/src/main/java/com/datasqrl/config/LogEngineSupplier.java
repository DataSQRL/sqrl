package com.datasqrl.config;

import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.engine.log.LogEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.google.inject.Inject;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor(onConstructor_=@Inject)
public class LogEngineSupplier {
  ExecutionPipeline pipeline;

  public LogEngine get() {
    return getOptional().get();
  }

  public boolean isPresent() {
    return getOptional().isPresent();
  }

  public Optional<LogEngine> getOptional() {
    return pipeline.getStage(Type.LOG)
        .map(stage -> (LogEngine) stage.get(0).getEngine());
  }
}
