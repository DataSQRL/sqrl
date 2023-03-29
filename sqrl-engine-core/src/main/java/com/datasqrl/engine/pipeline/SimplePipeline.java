/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.pipeline;

import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.stream.StreamEngine;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import java.util.List;
import java.util.Set;
import lombok.Value;

/**
 * Simple pipeline that does not support any branching (i.e. it's a straight line)
 */
@Value
public class SimplePipeline implements ExecutionPipeline {

  List<ExecutionStage> stages;

  HashMultimap<ExecutionStage, ExecutionStage> upstream = HashMultimap.create();
  HashMultimap<ExecutionStage, ExecutionStage> downstream = HashMultimap.create();

  private SimplePipeline(List<ExecutionStage> stages) {
    this.stages = stages;
    for (int i = 0; i < stages.size(); i++) {
      ExecutionStage stage = stages.get(i);
      for (int j = i; j < stages.size(); j++) {
        downstream.put(stage,stages.get(j));
      }
      for (int j = i; j >= 0 ; j--) {
        upstream.put(stage,stages.get(j));
      }
    }
  }

  public static SimplePipeline of(StreamEngine stream, DatabaseEngine db) {
    return new SimplePipeline(List.of(new EngineStage(stream), new EngineStage(db)));
  }


  @Override
  public Set<ExecutionStage> getUpStreamFrom(ExecutionStage stage) {
    Preconditions.checkArgument(upstream.containsKey(stage),"Invalid stage: %s",stage);
    return upstream.get(stage);
  }

  @Override
  public Set<ExecutionStage> getDownStreamFrom(ExecutionStage stage) {
    Preconditions.checkArgument(downstream.containsKey(stage),"Invalid stage: %s",stage);
    return downstream.get(stage);
  }
}
