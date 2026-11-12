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

import static com.google.common.base.Preconditions.checkState;

import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.global.StageAnalysis;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.dag.plan.MutationTable;
import com.datasqrl.planner.util.Documented.Documentation;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;

/** Represents a table in the DAG */
@Getter
public class TableNode extends PlannedNode {

  final TableAnalysis tableAnalysis;

  // The standalone include whose deployment subscribes to this table, or null
  private String subscribingInclude;

  public TableNode(TableAnalysis tableAnalysis, Map<ExecutionStage, StageAnalysis> stageAnalysis) {
    super(tableAnalysis.isSourceOrSink() ? "source" : "table", stageAnalysis);
    this.tableAnalysis = tableAnalysis;
  }

  /**
   * Returns the namespace of the standalone included project whose own deployment subscribes to
   * this table. That deployment writes the table to the log engine, so this project reads it from
   * there instead of writing it. Empty if no standalone include subscribes to the table.
   */
  public Optional<String> getSubscribingInclude() {
    return Optional.ofNullable(subscribingInclude);
  }

  /** Records that the deployment of the given standalone include subscribes to this table. */
  public void markSubscribedBy(@NonNull String standaloneInclude) {
    // A table is defined in the scripts of one include, so only that include subscribes to it
    checkState(
        subscribingInclude == null || subscribingInclude.equals(standaloneInclude),
        "Table %s is subscribed by the standalone includes %s and %s",
        getIdentifier(),
        subscribingInclude,
        standaloneInclude);
    subscribingInclude = standaloneInclude;
  }

  public boolean isSource() {
    // Table nodes cannot be sinks only
    return tableAnalysis.isSourceOrSink();
  }

  public boolean isSink() {
    // Mutation tables can be used as sinks
    return tableAnalysis
        .getSourceSinkTable()
        .map(t -> t.mutationDefinition() != null && t.mutationDefinition().isGenerateAccess())
        .orElse(false);
  }

  public Optional<MutationTable> getMutationTable() {
    if (!isSource()) {
      return Optional.empty();
    }
    MutationTable mutation = tableAnalysis.getSourceSinkTable().get().mutationDefinition();
    return Optional.ofNullable(mutation);
  }

  @Override
  public Documentation getDocumentation() {
    return tableAnalysis.getDocumentation();
  }

  @Override
  public TableAnalysis getAnalysis() {
    return tableAnalysis;
  }

  @Override
  public String getId() {
    return getIdentifier().toString();
  }
}
