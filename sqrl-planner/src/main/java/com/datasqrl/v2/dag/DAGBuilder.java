package com.datasqrl.v2.dag;

import com.datasqrl.v2.dag.nodes.ExportNode;
import com.datasqrl.v2.dag.nodes.PipelineNode;
import com.datasqrl.v2.dag.nodes.TableFunctionNode;
import com.datasqrl.v2.dag.nodes.TableNode;
import com.datasqrl.v2.tables.SqrlTableFunction;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.flink.table.catalog.ObjectIdentifier;

/**
 * {@link com.datasqrl.v2.SqlScriptPlanner} uses this builder to construct the DAG of table
 * and function definitions to build a {@link PipelineDAG}.
 */
public class DAGBuilder {

  Multimap<PipelineNode, PipelineNode> dagInputs = HashMultimap.create();
  Map<ObjectIdentifier, PipelineNode> nodeLookup = new HashMap<>();

  public void add(TableNode node) {
    PipelineNode priorNode = nodeLookup.put(node.getIdentifier(), node);
    //For AddColumn statements we need to replace the prior node in the DAG but we are guaranteed that no other node depends on it
    if (priorNode!=null) dagInputs.removeAll(priorNode);
    node.getTableAnalysis().getFromTables().forEach(inputTable ->
        dagInputs.put(node, Objects.requireNonNull(nodeLookup.get(inputTable.getIdentifier()))));
  }

  public void add(TableFunctionNode node) {
    SqrlTableFunction function = node.getFunction();
    if (function.getVisibility().isAccessOnly()) {
      //Remove prior function in case its an AddColumn statement or replacement of an access function/relationship
      dagInputs.removeAll(node);
    }
    nodeLookup.put(node.getIdentifier(), node);
    function.getFunctionAnalysis().getFromTables().forEach(inputTable ->
        dagInputs.put(node, Objects.requireNonNull(nodeLookup.get(inputTable.getIdentifier()))));
  }

  public PipelineDAG getDag() {
    return new PipelineDAG(dagInputs);
  }

  public Optional<PipelineNode> getNode(ObjectIdentifier identifier) {
    return Optional.ofNullable(nodeLookup.get(identifier));
  }

  public void addExport(ExportNode node, TableNode inputNode) {
    dagInputs.put(node, inputNode);
  }




}
