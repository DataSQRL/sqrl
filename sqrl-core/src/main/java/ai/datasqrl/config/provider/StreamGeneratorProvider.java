package ai.datasqrl.config.provider;

import ai.datasqrl.physical.stream.StreamEngine;

public interface StreamGeneratorProvider {

  StreamEngine.Generator create(StreamEngine envProvider, JDBCConnectionProvider jdbc);
}
