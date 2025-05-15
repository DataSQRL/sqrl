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

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.planner.dag.plan.ServerStagePlan;

/**
 * The server engine is a combination of the server core (the graphql engine) and the servlet that
 * is running it. Some servlets may not support things like Java, reflection, code generation
 * executors, etc.
 */
public interface ServerEngine extends ExecutionEngine {

  public EnginePhysicalPlan plan(ServerStagePlan serverPlan);
}
