package ai.dataeng.sqml.config.engines;

import ai.dataeng.sqml.execution.StreamEngine;
import ai.dataeng.sqml.execution.flink.environment.FlinkStreamEngineImpl;
import lombok.Builder;
import lombok.Getter;

import javax.annotation.Nullable;

@Builder
@Getter
public class FlinkConfiguration implements EngineConfiguration.Stream {

    @Nullable
    public Boolean savepoint;


    @Override
    public StreamEngine create() {
        return new FlinkStreamEngineImpl();
    }
}
