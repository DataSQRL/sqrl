/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.pipeline;

import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

  public static SimplePipeline of(Map<String, ExecutionEngine> engines, ErrorCollector errors) {
    //The ordering of stages is critical for simple pipeline
    List<ExecutionStage> stages = new ArrayList<>();
    getStage(Type.LOG, engines, false).ifPresent(stages::add);
    Optional<EngineStage> db = getStage(Type.DATABASE, engines, false);
    stages.add(getStage(Type.STREAMS, engines, db.isPresent()).orElseThrow(
        () -> errors.exception("Need to configure a stream engine")));
    db.ifPresent(stages::add);
    getStage(Type.SERVER, engines, false).ifPresent(stages::add);
    return new SimplePipeline(stages);
  }

  private static Optional<EngineStage> getStage(Type engineType,
      Map<String, ExecutionEngine> engines, boolean pullupOptimization) {
    return StreamUtil.getOnlyElement(engines.entrySet().stream()
        .filter(e -> e.getValue().getType()==engineType)
        .map(e -> new EngineStage(e.getKey(), e.getValue(), pullupOptimization)));

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
