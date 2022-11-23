package ai.datasqrl.config.provider;

import ai.datasqrl.physical.stream.StreamEngine;

public interface StreamEngineProvider {

  StreamEngine create();

}
