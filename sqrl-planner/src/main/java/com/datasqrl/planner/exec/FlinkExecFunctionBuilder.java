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
package com.datasqrl.planner.exec;

import com.datasqrl.planner.exec.FlinkExecFunction.Plan;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.CodeGenBridge;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

@Value
@Slf4j
public class FlinkExecFunctionBuilder {

  public static final String FUNCTON_NAME_FORMAT = "SqrlExec%s";

  FlinkTypeFactory typeFactory;
  TableConfig tableConfig;
  AtomicInteger fctCounter = new AtomicInteger();
  FlinkExecFunction.Plan.PlanBuilder planBuilder = FlinkExecFunction.Plan.builder();

  public FlinkExecFunction createFunction(
      RexNode expression, String description, RelDataType inputType) {
    return createFunction(List.of(expression), Optional.empty(), description, inputType);
  }

  public FlinkExecFunction createFunction(
      List<RexNode> expressions,
      Optional<RexNode> filter,
      String description,
      RelDataType inputType) {
    // 1) Translate type information
    RowType inRowType = (RowType) typeFactory.toLogicalType(inputType);
    RowType outRowType = toRowTypeFromRexNodes(expressions);

    String uniqueFunctionName = String.format(FUNCTON_NAME_FORMAT, fctCounter.getAndIncrement());
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    // Generate FlatMap function with Apache Flink Codegen
    GeneratedFunction<FlatMapFunction<RowData, RowData>> genFn =
        CodeGenBridge.gen(
            inRowType, // logical input type
            uniqueFunctionName, // class-name stub
            outRowType, // logical output type
            org.apache.flink.table.data.binary.BinaryRowData.class,
            // concrete RowData impl
            expressions, // Seq[RexNode]  –  projection
            filter, // Option[RexNode] – no filter
            tableConfig, // planner / code-gen settings
            cl); // class loader for Janino

    String functionCode = genFn.getCode();
    log.debug("Generated executable function [{}]: {}", uniqueFunctionName, functionCode);

    FlinkExecFunction fef =
        new FlinkExecFunction(uniqueFunctionName, description, inRowType, outRowType, genFn, null);
    planBuilder.function(fef);
    return fef;
  }

  public Plan getPlan() {
    return planBuilder.build();
  }

  private RowType toRowTypeFromRexNodes(List<RexNode> rexNodes) {
    LogicalType[] types =
        rexNodes.stream()
            .map(RexNode::getType)
            .map(FlinkTypeFactory::toLogicalType)
            .toArray(LogicalType[]::new);
    return RowType.of(false, types);
  }
}
