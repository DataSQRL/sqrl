package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.execution.StreamEngine;
import ai.dataeng.sqml.execution.flink.environment.FlinkStreamEngine;
import ai.dataeng.sqml.execution.flink.process.FlinkGenerator;

public interface StreamGeneratorProvider {
  StreamEngine.Generator create(StreamEngine envProvider, JDBCConnectionProvider jdbc);
}
