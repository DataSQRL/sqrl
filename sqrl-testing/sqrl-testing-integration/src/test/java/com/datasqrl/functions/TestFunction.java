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
package com.datasqrl.functions;

import com.google.auto.service.AutoService;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;

@AutoService(ScalarFunction.class)
public class TestFunction extends ScalarFunction {

  public boolean eval(Object... objects) {
    return true;
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    var inputTypeStrategy =
        InputTypeStrategies.compositeSequence().finishWithVarying(InputTypeStrategies.WILDCARD);

    return TypeInference.newBuilder()
        .inputTypeStrategy(inputTypeStrategy)
        .outputTypeStrategy(TypeStrategies.explicit(DataTypes.BOOLEAN()))
        .build();
  }
}
