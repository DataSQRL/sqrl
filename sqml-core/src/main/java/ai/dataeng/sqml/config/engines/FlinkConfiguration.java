package ai.dataeng.sqml.config.engines;

import ai.dataeng.sqml.execution.StreamEngine;
import ai.dataeng.sqml.execution.flink.environment.LocalFlinkStreamEngineImpl;
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
