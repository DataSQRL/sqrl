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
import com.datasqrl.planner.analyzer.TableOrFunctionAnalysis.UniqueIdentifier;
import java.util.Map;
import lombok.Getter;
import lombok.NonNull;

public abstract class PlannedNode extends PipelineNode {

  @Getter private final String documentation;

  public PlannedNode(
      @NonNull String type,
      Map<ExecutionStage, StageAnalysis> stageAnalysis,
      String documentation) {
    super(type, stageAnalysis);
    this.documentation = documentation;
  }

  public abstract TableAnalysis getAnalysis();

  public UniqueIdentifier getIdentifier() {
    return getAnalysis().getIdentifier();
  }
}
