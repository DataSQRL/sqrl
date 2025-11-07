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
package com.datasqrl.graphql.exec;

import com.datasqrl.graphql.exec.FlinkExecFunctionPlan.FlinkExecFunctionPlanBuilder;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

@Value
@Slf4j
public class FlinkExecFunctionFactory {

  public static final String FUNCTON_NAME_TEMPLATE = "SqrlExecFn%s";

  FlinkExecFunctionPlanBuilder planBuilder = FlinkExecFunctionPlan.builder();
  AtomicInteger fnCounter = new AtomicInteger();

  TableConfig tableConfig;
  FlinkTypeFactory typeFactory;

  public FlinkExecFunction create(RexNode expression, String description, RelDataType inputType) {
    return create(List.of(expression), Optional.empty(), description, inputType);
  }

  public FlinkExecFunction create(
      List<RexNode> expressions,
      Optional<RexNode> filter,
      String description,
      RelDataType inputType) {
    // Translate type information
    var inRowType = (RowType) typeFactory.toLogicalType(inputType);
    var outRowType = toRowTypeFromRexNodes(expressions);

    var uniqueFunctionName = String.format(FUNCTON_NAME_TEMPLATE, fnCounter.getAndIncrement());
    var cl = Thread.currentThread().getContextClassLoader();
    // Generate FlatMap function with Apache Flink Codegen
    var genFn =
        CodeGenBridge.gen(
            inRowType, // logical input type
            uniqueFunctionName, // class-name stub
            outRowType, // logical output type
            GenericRowData.class, // concrete RowData impl
            expressions, // Seq[RexNode]  –  projection
            filter, // Option[RexNode] – no filter
            tableConfig, // planner / code-gen settings
            cl); // class loader for Janino

    var fnCode = genFn.getCode();
    log.debug("Generated executable function [{}]: {}", uniqueFunctionName, fnCode);

    var execFn = new FlinkExecFunction(uniqueFunctionName, description, inRowType, genFn, null);

    planBuilder.function(execFn);

    return execFn;
  }

  public FlinkExecFunctionPlan getPlan() {
    return planBuilder.build();
  }

  private RowType toRowTypeFromRexNodes(List<RexNode> rexNodes) {
    var types =
        rexNodes.stream()
            .map(RexNode::getType)
            .map(FlinkTypeFactory::toLogicalType)
            .toArray(LogicalType[]::new);

    return RowType.of(false, types);
  }
}
