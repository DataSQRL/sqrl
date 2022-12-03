package com.datasqrl.physical.pipeline;

import com.datasqrl.physical.ExecutionEngine;
import com.datasqrl.physical.database.DatabaseEngine;
import com.datasqrl.physical.stream.StreamEngine;
import lombok.Value;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Simple pipeline that consists of a stream followed by a database
 */
@Value
public class EnginePipeline implements ExecutionPipeline {

    ExecutionStage streamStage;
    ExecutionStage dbStage;

    public EnginePipeline(DatabaseEngine db, StreamEngine stream) {
        dbStage = new EngineStage(db,Optional.empty());
        streamStage = new EngineStage(stream, Optional.of(dbStage));
    }

    @Override
    public Collection<ExecutionStage> getStages() {
        return List.of(streamStage,dbStage);
    }

    @Override
    public Optional<ExecutionStage> getStage(ExecutionEngine.Type type) {
        return getStages().stream().filter(s -> s.getEngine().getType()==type).findFirst();
    }

}
