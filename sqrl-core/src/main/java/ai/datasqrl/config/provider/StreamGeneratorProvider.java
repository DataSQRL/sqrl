package ai.datasqrl.config.provider;

import ai.datasqrl.execute.StreamEngine;

public interface StreamGeneratorProvider {
  StreamEngine.Generator create(StreamEngine envProvider, JDBCConnectionProvider jdbc);
}
