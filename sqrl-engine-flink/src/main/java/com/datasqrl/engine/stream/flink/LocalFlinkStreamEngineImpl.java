/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

import com.datasqrl.engine.stream.StreamEngine;
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
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(
        org.apache.flink.configuration.Configuration.fromMap(Map.of(
                "taskmanager.memory.network.fraction", "0.3",
                "taskmanager.memory.network.max", "1gb"
            )
        ));
    //env.getConfig().disableGenericTypes(); TODO: use to ensure efficient serialization
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
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
