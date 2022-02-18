package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.execution.StreamEngine;

public interface StreamEngineProvider {

    StreamEngine create();

}
