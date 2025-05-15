package com.datasqrl.engine.server;

import static com.datasqrl.engine.EngineFeature.NO_CAPABILITIES;

import com.datasqrl.config.EngineType;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;

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
  public EnginePhysicalPlan plan(com.datasqrl.planner.dag.plan.ServerStagePlan serverPlan) {
    serverPlan.getFunctions().stream().filter(fct -> fct.getExecutableQuery()==null).forEach(fct -> {
      throw new IllegalStateException("Function has not been planned: " + fct);
    });
    return new ServerPhysicalPlan(serverPlan.getFunctions(), serverPlan.getMutations(), null);
  }

}
