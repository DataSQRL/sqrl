package com.datasqrl.flinkwrapper.dag;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.flinkwrapper.dag.nodes.ExportNode;
import com.datasqrl.flinkwrapper.dag.nodes.PipelineNode;
import com.datasqrl.flinkwrapper.dag.nodes.TableNode;
import com.datasqrl.flinkwrapper.tables.AnnotatedSqrlTableFunction;
import com.datasqrl.plan.global.SqrlDAG.SqrlNode;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.flink.table.catalog.ObjectIdentifier;

public class DAGBuilder {

  Multimap<PipelineNode, PipelineNode> dagInputs = HashMultimap.create();
  Map<ObjectIdentifier, PipelineNode> nodeLookup = new HashMap<>();
  Map<NamePath, AnnotatedSqrlTableFunction> functions = new HashMap<>();

  public void add(TableNode node) {
    nodeLookup.put(node.getIdentifier(), node);
    node.getTableAnalysis().getFromTables().forEach(inputTable ->
        dagInputs.put(node, Objects.requireNonNull(nodeLookup.get(inputTable.getIdentifier()))));
  }

  public void add(AnnotatedSqrlTableFunction function) {
    functions.put(function.getFullPath(), function);
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
