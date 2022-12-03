package com.datasqrl.physical.stream.flink;

import com.datasqrl.config.error.ErrorCollector;
import com.datasqrl.config.util.ConfigurationUtil;
import com.datasqrl.physical.EngineConfiguration;
import com.datasqrl.physical.ExecutionEngine;
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
