/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

import java.util.Map;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class LocalFlinkStreamEngineImpl extends AbstractFlinkStreamEngine {

  public LocalFlinkStreamEngineImpl(FlinkEngineConfiguration config) {
    super(config);
  }

  public FlinkStreamBuilder createJob() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
        org.apache.flink.configuration.Configuration.fromMap(Map.of(
                //todo config
                "taskmanager.memory.network.fraction", "0.3",
                "taskmanager.memory.network.max", "1gb",
                "taskmanager.numberOfTaskSlots", "1",
                "parallelism.default", "1",
                "rest.flamegraph.enabled", "false"
            )
        ));
    env.getConfig().enableObjectReuse();
    //env.getConfig().disableGenericTypes(); TODO: use to ensure efficient serialization
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING); //todo add to config
//        FlinkUtilities.enableCheckpointing(env);

    return new FlinkStreamBuilder(this, env);
  }
}
