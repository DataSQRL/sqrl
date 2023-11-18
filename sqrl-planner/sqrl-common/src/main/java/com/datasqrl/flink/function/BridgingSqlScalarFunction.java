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

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.function.RuleTransform;
import lombok.Getter;
import org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.ReflectiveCallNotNullImplementor;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexFactory;
import org.apache.flink.table.types.inference.TypeInference;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Bridges a Flink function to calcite
 */
public class BridgingSqlScalarFunction extends SqlUserDefinedFunction implements BridgingFunction, RuleTransform {
  private final DataTypeFactory dataTypeFactory;
  private final FlinkTypeFactory flinkTypeFactory;
  private final RexFactory rexFactory;
  @Getter
  private final FunctionDefinition definition;
  private final TypeInference typeInference;

  public BridgingSqlScalarFunction(String name, DataTypeFactory dataTypeFactory,
                                   FlinkTypeFactory flinkTypeFactory, RexFactory rexFactory, SqlKind kind,
                                   FunctionDefinition definition, TypeInference typeInference) {
    super(
        new SqlIdentifier(name, SqlParserPos.ZERO),
        kind,
        createSqlReturnTypeInference(name, flinkTypeFactory, dataTypeFactory, definition),
        createSqlOperandTypeInference(name, flinkTypeFactory, dataTypeFactory, definition),
        createOperandMetadata(name, flinkTypeFactory, dataTypeFactory, definition),
        createCallableFlinkFunction(flinkTypeFactory, dataTypeFactory, definition),
        createSqlFunctionCategory());
    this.dataTypeFactory = dataTypeFactory;
    this.flinkTypeFactory = flinkTypeFactory;
    this.rexFactory = rexFactory;
    this.definition = definition;
    this.typeInference = typeInference;
  }

  private static SqlOperandMetadata createOperandMetadata(String name, FlinkTypeFactory flinkTypeFactory, DataTypeFactory dataTypeFactory, FunctionDefinition definition) {
    return new FlinkOperandMetadata(flinkTypeFactory, dataTypeFactory, definition, definition.getTypeInference(dataTypeFactory));
  }

  public static SqlReturnTypeInference createSqlReturnTypeInference(String name, FlinkTypeFactory flinkTypeFactory, DataTypeFactory dataTypeFactory, FunctionDefinition definition) {
    return new FlinkSqlReturnTypeInference(flinkTypeFactory, dataTypeFactory, definition, definition.getTypeInference(dataTypeFactory));
  }

  public static SqlOperandTypeInference createSqlOperandTypeInference(String name, FlinkTypeFactory flinkTypeFactory, DataTypeFactory dataTypeFactory, FunctionDefinition definition) {
    return new FlinkSqlOperandTypeInference(flinkTypeFactory, dataTypeFactory, definition, definition.getTypeInference(dataTypeFactory));
  }

  public static SqlOperandTypeChecker createSqlOperandTypeChecker(String name, FlinkTypeFactory flinkTypeFactory, DataTypeFactory dataTypeFactory, FunctionDefinition definition) {
    return new FlinkSqlOperandTypeChecker(flinkTypeFactory, dataTypeFactory, definition, definition.getTypeInference(dataTypeFactory));
  }

  public static SqlFunctionCategory createSqlFunctionCategory() {
    return SqlFunctionCategory.USER_DEFINED_FUNCTION;
  }

  @Override
  public List<String> getParamNames() {
    if (typeInference.getNamedArguments().isPresent()) {
      return typeInference.getNamedArguments().get();
    }
    return super.getParamNames();
  }

  @Override
  public boolean isDeterministic() {
    return definition.isDeterministic();
  }


  private static Function createCallableFlinkFunction(FlinkTypeFactory flinkTypeFactory, DataTypeFactory dataTypeFactory, FunctionDefinition definition) {
    return new ImplementableFunction() {
      //Handles execution to flink code
      @Override
      public CallImplementor getImplementor() {
        Class[] params = definition.getTypeInference(dataTypeFactory)
            .getTypedArguments().stream()
            .flatMap(a->a.stream())
            .map(a->a.getConversionClass())
            .toArray(s -> new Class[s]);

        // Get the method handle for the eval method
        Method method;
        try {
          method = definition.getClass().getMethod("eval", params);
        } catch (NoSuchMethodException e) {
          throw new RuntimeException("Method eval not found in class:" + definition, e);
        }

        return createImplementor(method);
      }

      private CallImplementor createImplementor(final Method method) {
        final NullPolicy nullPolicy = NullPolicy.SEMI_STRICT;
        return RexImpTable.createImplementor(
            new ReflectiveCallNotNullImplementor(method), nullPolicy, false);
      }

      @Override
      public List<FunctionParameter> getParameters() {
        //derive parameters (necessary?)
        return List.of();
      }
    };
  }

  @Override
  public List<RelRule> transform(Dialect dialect, SqlOperator operator) {
    if (definition instanceof RuleTransform) {
      return ((RuleTransform) definition).transform(dialect, this);
    }

    return List.of();
  }


  @Override
  public String getRuleOperatorName() {
    if (definition instanceof RuleTransform) {
      return ((RuleTransform) definition).getRuleOperatorName();
    }
    return null;
  }
}