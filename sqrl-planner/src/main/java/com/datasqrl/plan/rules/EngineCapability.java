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
import org.apache.calcite.sql.SqlOperator;

public interface EngineCapability {

  String getName();

  record Feature(EngineFeature feature) implements EngineCapability {

    @Override
    public String getName() {
      return feature.name() + " (feature)";
    }

    @Override
    public String toString() {
      return getName();
    }
  }

  record Function(SqlOperator function) implements EngineCapability {

    @Override
    public String getName() {
      return function.getName() + " (function)";
    }

    @Override
    public String toString() {
      return getName();
    }
  }
}
