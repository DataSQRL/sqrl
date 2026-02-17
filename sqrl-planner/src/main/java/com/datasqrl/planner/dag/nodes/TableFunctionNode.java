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
package com.datasqrl.planner.dag.nodes;

import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.global.StageAnalysis;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.tables.SqrlTableFunction;
import java.util.Map;
import lombok.Getter;

/** Represents a table function in the DAG */
@Getter
public class TableFunctionNode extends PlannedNode {

  private final SqrlTableFunction function;

  public TableFunctionNode(
      SqrlTableFunction function,
      Map<ExecutionStage, StageAnalysis> stageAnalysis,
      String documentation) {
    super("function", stageAnalysis, documentation);
    this.function = function;
  }

  @Override
  public String getId() {
    /*
    Access only functions are not planned and therefore do not have a unique object identifier,
    instead they are identified by their full path
     */
    return function.getVisibility().isAccessOnly()
        ? "access:" + function.getFullPath()
        : function.getIdentifier().toString();
    //        + "(" + function.getParameters().stream().map(
    //        FunctionParameter::getName).collect(Collectors.joining(",")) + ")";
  }

  @Override
  public boolean isSink() {
    return function.getVisibility().isEndpoint();
  }

  @Override
  public TableAnalysis getAnalysis() {
    return function.getFunctionAnalysis();
  }
}
