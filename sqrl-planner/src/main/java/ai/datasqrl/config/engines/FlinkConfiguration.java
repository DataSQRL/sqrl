package ai.datasqrl.config.engines;

import ai.datasqrl.physical.stream.StreamEngine;
import ai.datasqrl.physical.stream.flink.LocalFlinkStreamEngineImpl;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class FlinkConfiguration implements EngineConfiguration.Stream {

  @Builder.Default
  boolean savepoint = false;

  @Override
  public StreamEngine create() {
    return new LocalFlinkStreamEngineImpl();
  }
}
