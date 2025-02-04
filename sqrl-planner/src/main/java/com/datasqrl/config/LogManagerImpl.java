package com.datasqrl.config;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.engine.log.Log;
import com.datasqrl.engine.log.LogEngine;
import com.datasqrl.engine.log.LogFactory;
import com.datasqrl.engine.log.LogManager;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.google.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Central manager for log creation.
 */
public class LogManagerImpl implements LogManager {

  Optional<LogFactory> proxiedFactory;
  @Getter
  Map<String, Log> logs = new HashMap<>();

  @Inject
  public LogManagerImpl(ExecutionPipeline pipeline) {
    proxiedFactory = pipeline.getStageByType(EngineType.LOG)
        .map(stage -> (LogEngine) stage.getEngine())
        .map(LogEngine::getLogFactory);
  }

  @Override
  public boolean hasLogEngine() {
    return proxiedFactory.isPresent();
  }

  @Override
  public Log create(String logId, Name logName, RelDataType schema, List<String> primaryKey,
      Timestamp timestamp) {
    Log log = proxiedFactory.orElseThrow(() -> new IllegalStateException("No log engine configured"))
        .create(logId, logName, schema, primaryKey, timestamp);
    logs.put(logId, log);
    return log;
  }

  @Override
  public String getEngineName() {
    return proxiedFactory.get().getEngineName();
  }
}
