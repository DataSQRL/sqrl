/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.engine.server;

import static com.datasqrl.engine.EngineFeature.NO_CAPABILITIES;

import com.datasqrl.config.EngineType;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import lombok.extern.slf4j.Slf4j;

/** A generic java server engine. */
@Slf4j
public abstract class GenericJavaServerEngine extends ExecutionEngine.Base implements ServerEngine {

  public GenericJavaServerEngine(String engineName) {
    super(engineName, EngineType.SERVER, NO_CAPABILITIES);
  }

  @Override
  public EnginePhysicalPlan plan(com.datasqrl.planner.dag.plan.ServerStagePlan serverPlan) {
    serverPlan.getFunctions().stream()
        .filter(fct -> fct.getExecutableQuery() == null)
        .forEach(
            fct -> {
              throw new IllegalStateException("Function has not been planned: " + fct);
            });
    return new ServerPhysicalPlan(serverPlan.getFunctions(), serverPlan.getMutations(), null);
  }
}
