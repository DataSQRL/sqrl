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
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.adapter.enumerable.AggAddContext;
import org.apache.calcite.adapter.enumerable.AggContext;
import org.apache.calcite.adapter.enumerable.AggImplementor;
import org.apache.calcite.adapter.enumerable.AggResetContext;
import org.apache.calcite.adapter.enumerable.AggResultContext;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.AggregateFunction;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ImplementableAggFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.util.Optionality;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexFactory;
import org.apache.flink.table.planner.functions.inference.TypeInferenceOperandInference;
import org.apache.flink.table.types.inference.TypeInference;

/**
 * Bridges a Flink function to calcite
 */
public class BridgingSqlAggregateFunction extends SqlUserDefinedAggFunction implements BridgingFunction, RuleTransform {
  @Getter
  private final FunctionDefinition definition;
  private final TypeInference typeInference;

  public BridgingSqlAggregateFunction(String name, DataTypeFactory dataTypeFactory,
                                   FlinkTypeFactory flinkTypeFactory, RexFactory rexFactory, SqlKind kind,
      FunctionDefinition definition, TypeInference typeInference) {
    super(
        new SqlIdentifier(name, SqlParserPos.ZERO),
        kind,
        createSqlReturnTypeInference(name, flinkTypeFactory, dataTypeFactory, definition),
        createSqlOperandTypeInference(name, flinkTypeFactory, dataTypeFactory, definition),
        createOperandMetadata(name, flinkTypeFactory, dataTypeFactory, definition),
        createCallableFlinkFunction(flinkTypeFactory, dataTypeFactory, definition),
        false,
        false,
        Optionality.IGNORED);
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
    return new FlinkSqlOperandTypeInference(flinkTypeFactory, new TypeInferenceOperandInference(dataTypeFactory, definition, definition.getTypeInference(dataTypeFactory)));
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

  private static AggregateFunction createCallableFlinkFunction(FlinkTypeFactory flinkTypeFactory, DataTypeFactory dataTypeFactory, FunctionDefinition definition) {
//    Class clazz = definition.getClass();
//    Method initMethod = ReflectionUtil.findMethod(clazz, "createAccumulator");
//    Method addMethod = ReflectionUtil.findMethod(clazz, "accumulate");
//    Method mergeMethod = ReflectionUtil.findMethod(clazz, "merge");;
//    Method resultMethod = ReflectionUtil.findMethod(clazz, "result");
//    Class<?> accumulatorType = initMethod.getReturnType();
//    Class<?> resultType = resultMethod != null ? resultMethod.getReturnType() : accumulatorType;
//    List<Class> addParamTypes = ImmutableList.copyOf((Class[])addMethod.getParameterTypes());
//    if (!addParamTypes.isEmpty() && addParamTypes.get(0) == accumulatorType) {
//      ReflectiveFunctionBase.ParameterListBuilder params = ReflectiveFunctionBase.builder();
//      ImmutableList.Builder<Class<?>> valueTypes = ImmutableList.builder();
//
//      for (int i = 1; i < addParamTypes.size(); ++i) {
//        Class type = (Class) addParamTypes.get(i);
//        String name = ReflectUtil.getParameterName(addMethod, i);
//        boolean optional = ReflectUtil.isParameterOptional(addMethod, i);
//        params.add(type, name, optional);
//        valueTypes.add(type);
//      }
//    }

    return new ImplementableAggFunction() {

      @Override
      public List<FunctionParameter> getParameters() {
        return List.of();
      }

      @Override
      public RelDataType getReturnType(RelDataTypeFactory relDataTypeFactory) {
        return null;
      }

      @Override
      public AggImplementor getImplementor(boolean b) {
        return new AggImplementor() {
          @Override
          public List<Type> getStateType(AggContext aggContext) {
            return null;
          }

          @Override
          public void implementReset(AggContext aggContext, AggResetContext aggResetContext) {

          }

          @Override
          public void implementAdd(AggContext aggContext, AggAddContext aggAddContext) {

          }

          @Override
          public Expression implementResult(AggContext aggContext,
              AggResultContext aggResultContext) {
            return null;
          }
        };
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