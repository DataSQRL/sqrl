package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.execution.StreamEngine;

public interface StreamGeneratorProvider {
  StreamEngine.Generator create(StreamEngine envProvider, JDBCConnectionProvider jdbc);
}
