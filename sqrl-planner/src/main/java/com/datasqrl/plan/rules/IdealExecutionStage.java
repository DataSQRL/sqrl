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
package com.datasqrl.plan.rules;

import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.pipeline.ExecutionStage;
import lombok.Value;

@Value
public final class IdealExecutionStage implements ExecutionStage {

  public static final ExecutionStage INSTANCE = new IdealExecutionStage();
  public static final String NAME = "IDEAL";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean supportsFeature(EngineFeature capability) {
    return true;
  }

  @Override
  public ExecutionEngine getEngine() {
    throw new UnsupportedOperationException();
  }
}
