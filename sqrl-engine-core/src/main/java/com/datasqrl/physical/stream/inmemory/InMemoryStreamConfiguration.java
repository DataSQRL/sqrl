package ai.datasqrl.physical.stream.inmemory;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.physical.EngineConfiguration;
import ai.datasqrl.physical.ExecutionEngine;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Builder
@Getter
@NoArgsConstructor
public class InMemoryStreamConfiguration implements EngineConfiguration {

    public static final String ENGINE_NAME = "memStream";

    @Override
    public String getEngineName() {
        return ENGINE_NAME;
    }

    @Override
    public ExecutionEngine.Type getEngineType() {
        return ExecutionEngine.Type.STREAM;
    }

    @Override
    public InMemStreamEngine initialize(@NonNull ErrorCollector errors) {
        return new InMemStreamEngine();
    }
}
