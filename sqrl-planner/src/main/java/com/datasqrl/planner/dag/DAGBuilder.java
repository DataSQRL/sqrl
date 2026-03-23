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
package com.datasqrl.planner.dag;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datasqrl.planner.analyzer.TableOrFunctionAnalysis;
import com.datasqrl.planner.analyzer.TableOrFunctionAnalysis.UniqueIdentifier;
import com.datasqrl.planner.dag.nodes.ExportNode;
import com.datasqrl.planner.dag.nodes.PipelineNode;
import com.datasqrl.planner.dag.nodes.PlannedNode;
import com.datasqrl.planner.dag.nodes.TableFunctionNode;
import com.datasqrl.planner.dag.nodes.TableNode;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.table.catalog.ObjectIdentifier;

/**
 * {@link com.datasqrl.planner.SqlScriptPlanner} uses this builder to construct the DAG of table and
 * function definitions to build a {@link PipelineDAG}.
 */
public class DAGBuilder {

  Multimap<PipelineNode, PipelineNode> dagInputs = HashMultimap.create();
  Map<UniqueIdentifier, PlannedNode> nodeLookup = new HashMap<>();

  public void add(TableNode node) {
    var priorNode = nodeLookup.put(node.getIdentifier(), node);
    // For AddColumn statements we need to replace the prior node in the DAG but we are guaranteed
    // that no other node depends on it
    if (priorNode != null) {
      dagInputs.removeAll(priorNode);
    }

    var fromTables = node.getTableAnalysis().getFromTables();
    for (var inputTable : fromTables) {
      addInputTable(node, inputTable);
    }
  }

  public void add(TableFunctionNode node) {
    var function = node.getFunction();
    if (function.getVisibility().isAccessOnly()) {
      // Remove prior function in case its an AddColumn statement or replacement of an access
      // function/relationship
      dagInputs.removeAll(node);
    }

    nodeLookup.put(node.getIdentifier(), node);

    var fromTables = function.getFunctionAnalysis().getFromTables();
    for (var inputTable : fromTables) {
      addInputTable(node, inputTable);
    }
  }

  public PipelineDAG getDag() {
    return new PipelineDAG(dagInputs);
  }

  public Optional<PipelineNode> getNode(ObjectIdentifier identifier) {
    return getNode(new UniqueIdentifier(identifier, false));
  }

  public Optional<PipelineNode> getNode(UniqueIdentifier identifier) {
    return Optional.ofNullable(nodeLookup.get(identifier));
  }

  public void addExport(ExportNode node, TableNode inputNode) {
    dagInputs.put(node, inputNode);
  }

  private void addInputTable(PlannedNode node, TableOrFunctionAnalysis inputTable) {
    var inputNode = nodeLookup.get(inputTable.getIdentifier());
    checkNotNull(inputNode, "Planned node not found by id: " + inputTable.getIdentifier());

    dagInputs.put(node, inputNode);
  }
}
