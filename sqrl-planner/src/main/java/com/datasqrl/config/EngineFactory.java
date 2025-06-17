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
package com.datasqrl.config;

import com.datasqrl.engine.IExecutionEngine;
import java.util.Set;

public interface EngineFactory {
  String ENGINE_NAME_KEY = "type";
  Set<String> RESERVED_KEYS = Set.of(ENGINE_NAME_KEY);

  static Set<String> getReservedKeys() {
    return RESERVED_KEYS;
  }

  String getEngineName();

  EngineType getEngineType();

  Class<? extends IExecutionEngine> getFactoryClass();
}
