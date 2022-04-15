package ai.datasqrl.config.provider;

import ai.datasqrl.execute.StreamEngine;

public interface StreamEngineProvider {

    StreamEngine create();

}
