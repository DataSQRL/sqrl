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

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.global.StageAnalysis;
import com.datasqrl.planner.tables.FlinkConnectorConfig;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.apache.flink.table.catalog.ObjectIdentifier;

/** Represents an EXPORT statement in the DAG */
@Getter
public class ExportNode extends PipelineNode {

  private final NamePath sinkPath;
  private final int batchIndex;

  private final Optional<ExecutionStage> sinkTo;
  private final Optional<ObjectIdentifier> createdSinkTable;
  private final Optional<FlinkConnectorConfig> connectorConfig;

  public ExportNode(
      Map<ExecutionStage, StageAnalysis> stageAnalysis,
      NamePath sinkPath,
      int batchIndex,
      Optional<ExecutionStage> sinkTo,
      Optional<ObjectIdentifier> createdSinkTable,
      Optional<FlinkConnectorConfig> connectorConfig) {
    super("export", stageAnalysis);
    this.sinkPath = sinkPath;
    this.batchIndex = batchIndex;
    this.sinkTo = sinkTo;
    this.createdSinkTable = createdSinkTable;
    this.connectorConfig = connectorConfig;
  }

  @Override
  public boolean isSink() {
    return true;
  }

  @Override
  public String getId() {
    return sinkPath.toString();
  }
}
