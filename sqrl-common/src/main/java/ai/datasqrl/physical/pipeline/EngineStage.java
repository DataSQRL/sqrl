package ai.datasqrl.physical.pipeline;

import ai.datasqrl.config.engines.JDBCConfiguration;
import ai.datasqrl.physical.EngineCapability;
import ai.datasqrl.physical.ExecutionEngine;
import ai.datasqrl.physical.database.relational.JDBCEngine;
import ai.datasqrl.physical.stream.flink.FlinkStreamEngine;
import lombok.Value;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Value
public class EngineStage implements ExecutionStage {

    ExecutionEngine engine;
    Optional<ExecutionStage> next;

    @Override
    public String getName() {
        return engine.getName();
    }

    @Override
    public boolean supports(EngineCapability capability) {
        return engine.supports(capability);
    }

    @Override
    public Optional<ExecutionStage> nextStage() {
        return next;
    }

    //TODO: use configuration to build this
    public static final ExecutionPipeline streamDatabasePipeline() {
        ExecutionStage db = new EngineStage(new JDBCEngine(JDBCConfiguration.Dialect.POSTGRES),Optional.empty());
        ExecutionStage stream = new EngineStage(FlinkStreamEngine.STANDARD, Optional.of(db));
        return new ExecutionPipeline() {

            @Override
            public Collection<ExecutionStage> getStages() {
                return List.of(stream,db);
            }

            @Override
            public Optional<ExecutionStage> getStage(ExecutionEngine.Type type) {
                return getStages().stream().filter(s -> s.getEngine().getType()==type).findFirst();
            }
        };
    }
}
