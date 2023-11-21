/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//Copied from flink as we incrementally phase out flink code for sqrl code
package com.datasqrl.flink.function;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeTransform;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.inference.TypeInferenceReturnInference;
import org.apache.flink.table.types.inference.TypeInference;

public class FlinkSqlReturnTypeInference implements SqlReturnTypeInference {

  private final FlinkTypeFactory flinkTypeFactory;
  private final TypeInferenceReturnInference returnTypeInference;

  public FlinkSqlReturnTypeInference(
      FlinkTypeFactory flinkTypeFactory, DataTypeFactory dataTypeFactory,
      FunctionDefinition definition,
      TypeInference typeInference) {
    this.flinkTypeFactory = flinkTypeFactory;
    this.returnTypeInference = new TypeInferenceReturnInference(dataTypeFactory, definition, typeInference);
  }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    SqlOperatorBinding binding = wrapSqlOperatorBinding(opBinding);
    return returnTypeInference.inferReturnType(binding);
  }

  private SqlOperatorBinding wrapSqlOperatorBinding(SqlOperatorBinding opBinding) {
    return new DelegatingSqlOperatorBinding(flinkTypeFactory, opBinding);
  }

  @Override
  public SqlReturnTypeInference andThen(SqlTypeTransform transform) {
    return returnTypeInference.andThen(transform);
  }

  @Override
  public SqlReturnTypeInference orElse(SqlReturnTypeInference transform) {
    return returnTypeInference.orElse(transform);
  }
}
