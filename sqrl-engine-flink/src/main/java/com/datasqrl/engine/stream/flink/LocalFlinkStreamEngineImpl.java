/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

import com.datasqrl.engine.stream.StreamEngine;
import com.datasqrl.io.DataSystemConnectorConfig;
import lombok.NonNull;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class LocalFlinkStreamEngineImpl extends AbstractFlinkStreamEngine {

  private final ConcurrentHashMap<String, LocalJob> jobs = new ConcurrentHashMap<>();

  public LocalFlinkStreamEngineImpl(FlinkEngineConfiguration config) {
    super(config);
  }

  @Override
  public FlinkStreamBuilder createJob() {


    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
        org.apache.flink.configuration.Configuration.fromMap(Map.of(
            //todo config
                "taskmanager.memory.network.fraction", "0.3",
                "taskmanager.memory.network.max", "1gb",
                "taskmanager.numberOfTaskSlots", "32",
                "parallelism.default", "32",
            "table.exec.mini-batch.enabled", "true",
            "table.exec.mini-batch.allow-latency", "5 s",
            "table.exec.mini-batch.size", "5000",
            "rest.flamegraph.enabled", "true"
            )
        ));
    env.getConfig().enableObjectReuse();
    //env.getConfig().disableGenericTypes(); TODO: use to ensure efficient serialization
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING); //todo add to config
    //            .forEach(e->registerFunc(e, catalog));
//        FlinkUtilities.enableCheckpointing(env);
    return new FlinkStreamBuilder(this, env);
  }

  @Override
  public FlinkJob createStreamJob(@NonNull StreamExecutionEnvironment execEnv,
      @NonNull JobType type) {
    return new LocalJob(execEnv, type);
  }

  @Override
  public Optional<StreamEngine.Job> getJob(String id) {
    StreamEngine.Job job = jobs.get(id);
    return Optional.ofNullable(job);
  }

  @Override
  public void close() throws IOException {
    jobs.clear();
  }

  @Override
  public DataSystemConnectorConfig getDataSystemConnectorConfig() {
    return null;
  }

  class LocalJob extends FlinkJob {

    protected LocalJob(StreamExecutionEnvironment execEnv, JobType type) {
      super(execEnv, type);
    }

    @Override
    public void execute(String name) {
      super.execute(name);
      if (status != Status.FAILED) {
        jobs.put(getId(), this);
      }
    }
  }
}
