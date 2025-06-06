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
package com.datasqrl.engine.pipeline;

import com.datasqrl.config.EngineType;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.ExecutionEngine;
import java.util.Collection;
import org.apache.calcite.sql.SqlOperator;

public interface ExecutionStage {

  String getName();

  default boolean supportsAllFeatures(Collection<EngineFeature> capabilities) {
    return capabilities.stream().allMatch(this::supportsFeature);
  }

  default EngineType getType() {
    return getEngine().getType();
  }

  boolean supportsFeature(EngineFeature capability);

  //  boolean supportsFunction(FunctionDefinition function);

  default boolean isRead() {
    return getEngine().getType().isRead();
  }

  default boolean isWrite() {
    return getEngine().getType().isWrite();
  }

  default boolean isCompute() {
    return getEngine().getType().isCompute();
  }

  ExecutionEngine getEngine();

  default boolean supportsFunction(SqlOperator operator) {
    return true;
  }
}
