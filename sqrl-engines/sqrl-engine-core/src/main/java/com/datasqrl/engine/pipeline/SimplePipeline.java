/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.pipeline;

import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.server.ServerEngine;
import com.datasqrl.engine.stream.StreamEngine;
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
    List<ExecutionStage> stages = new ArrayList<>();
    //todo: this logic is too hidden
    stages.add(getStage(Type.STREAM, engines).orElseThrow(
        () -> errors.exception("Need to configure a stream engine")));
    stages.add(getStage(Type.DATABASE, engines).orElseThrow(
        () -> errors.exception("Need to configure a database engine")));
    getStage(Type.SERVER, engines).ifPresent(stages::add);
    return new SimplePipeline(stages);
  }

  private static Optional<EngineStage> getStage(ExecutionEngine.Type engineType,
      Map<String, ExecutionEngine> engines) {
    return StreamUtil.getOnlyElement(engines.entrySet().stream()
        .filter(e -> e.getValue().getType()==engineType)
        .map(e -> new EngineStage(e.getKey(),e.getValue())));

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
