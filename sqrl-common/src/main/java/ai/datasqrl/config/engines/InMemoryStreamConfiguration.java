package ai.datasqrl.config.engines;

import ai.datasqrl.physical.stream.StreamEngine;
import ai.datasqrl.physical.stream.flink.LocalFlinkStreamEngineImpl;
import ai.datasqrl.physical.stream.inmemory.InMemStreamEngine;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@NoArgsConstructor
public class InMemoryStreamConfiguration implements EngineConfiguration.Stream {

    @Override
    public StreamEngine create() {
        return new InMemStreamEngine();
    }

}
