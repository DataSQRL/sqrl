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
package com.datasqrl.engine;

import com.datasqrl.config.EngineType;
import java.util.EnumSet;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

/** Describes a physical execution engine and it's capabilities. */
public interface ExecutionEngine extends IExecutionEngine {

  /**
   * @param capability
   * @return whether the engine supports the given {@link EngineFeature}
   */
  boolean supports(EngineFeature capability);

  @AllArgsConstructor
  @Getter
  abstract class Base implements ExecutionEngine {

    protected final @NonNull String name;
    protected final @NonNull EngineType type;
    protected final @NonNull EnumSet<EngineFeature> capabilities;

    @Override
    public boolean supports(EngineFeature capability) {
      return capabilities.contains(capability);
    }
  }
}
