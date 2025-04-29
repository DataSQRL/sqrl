package com.datasqrl.engine.server;

import static com.datasqrl.engine.EngineFeature.NO_CAPABILITIES;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.EngineType;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.PhysicalDAGPlan.ServerStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

/**
 * A generic java server engine.
 */
@Slf4j
public abstract class GenericJavaServerEngine extends ExecutionEngine.Base implements ServerEngine {

  public GenericJavaServerEngine(String engineName) {
    super(engineName, EngineType.SERVER, NO_CAPABILITIES);
  }

  @Override
  public EnginePhysicalPlan plan(com.datasqrl.v2.dag.plan.ServerStagePlan serverPlan) {
    serverPlan.getFunctions().stream().filter(fct -> fct.getExecutableQuery()==null).forEach(fct -> {
      throw new IllegalStateException("Function has not been planned: " + fct);
    });
    return new ServerPhysicalPlan(serverPlan.getFunctions(), serverPlan.getMutations(), null);
  }

}
