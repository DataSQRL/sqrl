package com.datasqrl.flinkwrapper.dag;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.flinkwrapper.analyzer.TableAnalysis;
import com.datasqrl.flinkwrapper.dag.nodes.ExportNode;
import com.datasqrl.flinkwrapper.dag.nodes.PipelineNode;
import com.datasqrl.flinkwrapper.dag.nodes.TableFunctionNode;
import com.datasqrl.flinkwrapper.dag.nodes.TableNode;
import com.datasqrl.flinkwrapper.tables.SqrlTableFunction;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import lombok.Getter;
import org.apache.flink.table.catalog.ObjectIdentifier;

public class DAGBuilder {

  Multimap<PipelineNode, PipelineNode> dagInputs = HashMultimap.create();
  Map<ObjectIdentifier, PipelineNode> nodeLookup = new HashMap<>();
  @Getter
  Map<NamePath, SqrlTableFunction> apiFunctions = new HashMap<>();
  @Getter
  Map<NamePath, TableAnalysis> apiMutations = new HashMap<>();


  public void add(TableNode node) {
    nodeLookup.put(node.getIdentifier(), node);
    node.getTableAnalysis().getFromTables().forEach(inputTable ->
        dagInputs.put(node, Objects.requireNonNull(nodeLookup.get(inputTable.getIdentifier()))));
    if (node.isMutation()) {
      apiMutations.put(NamePath.of(node.getIdentifier().getObjectName()), node.getTableAnalysis());
    }
  }

  public void add(TableFunctionNode node) {
    if (!node.getFunction().getVisibility().isAccessOnly()) {
      nodeLookup.put(node.getIdentifier(), node);
    }
    node.getFunction().getFunctionAnalysis().getFromTables().forEach(inputTable ->
        dagInputs.put(node, Objects.requireNonNull(nodeLookup.get(inputTable.getIdentifier()))));
    if (!node.getFunction().getVisibility().isHidden()) {
      apiFunctions.put(node.getFunction().getFullPath(), node.getFunction());
    }
  }

  public void addRelationship(SqrlTableFunction function) {
    apiFunctions.put(function.getFullPath(), function);
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
