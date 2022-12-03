package com.datasqrl.engine.stream.flink;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ConfigurationUtil;
import com.datasqrl.engine.EngineConfiguration;
import com.datasqrl.engine.ExecutionEngine;
import lombok.*;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class FlinkEngineConfiguration implements EngineConfiguration {

  public static final String ENGINE_NAME = "flink";

  @Builder.Default
  boolean savepoint = false;

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public ExecutionEngine.Type getEngineType() {
    return ExecutionEngine.Type.STREAM;
  }

  @Override
  public FlinkStreamEngine initialize(@NonNull ErrorCollector errors) {
    ConfigurationUtil.javaxValidate(this,errors);
    return new LocalFlinkStreamEngineImpl(this);
  }

}
