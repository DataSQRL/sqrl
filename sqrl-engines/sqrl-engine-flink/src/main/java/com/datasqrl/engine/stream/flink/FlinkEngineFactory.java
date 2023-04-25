package com.datasqrl.engine.stream.flink;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigUtil;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.ExecutionEngine;
import com.google.auto.service.AutoService;
import java.util.HashMap;
import java.util.Map;
import lombok.NonNull;

@AutoService(EngineFactory.class)
public class FlinkEngineFactory implements EngineFactory {

  public static final String ENGINE_NAME = "flink";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public ExecutionEngine.Type getEngineType() {
    return ExecutionEngine.Type.STREAM;
  }

  @Override
  public AbstractFlinkStreamEngine initialize(@NonNull SqrlConfig config) {
    return new LocalFlinkStreamEngineImpl(new ExecutionEnvironmentFactory(getFlinkConfiguration(config)));
  }

  public Map<String,String> getFlinkConfiguration(@NonNull SqrlConfig config) {
    return SqrlConfigUtil.toStringMap(config, EngineFactory.getReservedKeys());
  }

}
