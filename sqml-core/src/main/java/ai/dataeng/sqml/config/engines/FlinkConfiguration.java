package ai.dataeng.sqml.config.engines;

import ai.dataeng.sqml.execution.StreamEngine;
import ai.dataeng.sqml.execution.flink.environment.FlinkStreamEngineImpl;
import lombok.*;

import javax.validation.constraints.NotNull;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class FlinkConfiguration implements EngineConfiguration.Stream {

    @Builder.Default
    boolean savepoint = false;

    @Override
    public StreamEngine create() {
        return new FlinkStreamEngineImpl();
    }
}
