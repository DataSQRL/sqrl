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

import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType;
import static org.apache.flink.table.types.inference.TypeInferenceUtil.adaptArguments;
import static org.apache.flink.table.types.inference.TypeInferenceUtil.createInvalidCallException;
import static org.apache.flink.table.types.inference.TypeInferenceUtil.createInvalidInputException;
import static org.apache.flink.table.types.inference.TypeInferenceUtil.createUnexpectedException;
import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsAvoidingCast;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.inference.ArgumentCountRange;
import org.apache.flink.table.planner.functions.inference.CallBindingCallContext;
import org.apache.flink.table.planner.functions.inference.TypeInferenceOperandChecker;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeInferenceUtil;
import org.apache.flink.table.types.logical.LogicalType;


public class FlinkSqlOperandTypeChecker implements SqlOperandTypeChecker {

  private final FlinkTypeFactory flinkTypeFactory;
   private final TypeInferenceOperandChecker typeChecker;

  public FlinkSqlOperandTypeChecker(
      FlinkTypeFactory flinkTypeFactory, DataTypeFactory dataTypeFactory,
      FunctionDefinition definition,
      TypeInference typeInference) {
    this.flinkTypeFactory = flinkTypeFactory;
    this.typeChecker = new TypeInferenceOperandChecker(dataTypeFactory, definition, typeInference);
  }

  @Override
  public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
    DelegatingSqlCallBinding delegatingSqlCallBinding = new DelegatingSqlCallBinding(flinkTypeFactory, callBinding);
    return typeChecker.checkOperandTypes(delegatingSqlCallBinding, throwOnFailure);
  }

  @Override
  public SqlOperandCountRange getOperandCountRange() {
    return typeChecker.getOperandCountRange();
  }

  @Override
  public String getAllowedSignatures(SqlOperator op, String opName) {
    return typeChecker.getAllowedSignatures(op, opName);
  }

  @Override
  public Consistency getConsistency() {
    return typeChecker.getConsistency();
  }

  @Override
  public boolean isOptional(int i) {
    return typeChecker.isOptional(i);
  }

  @Override
  public boolean isFixedParameters() {
    return typeChecker.isFixedParameters();
  }
}
