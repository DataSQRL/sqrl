package com.datasqrl.flinkwrapper.dag;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.flinkwrapper.dag.nodes.PipelineNode;
import com.datasqrl.flinkwrapper.tables.AnnotatedSqrlTableFunction;
import com.datasqrl.plan.global.SqrlDAG.SqrlNode;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.table.catalog.ObjectIdentifier;

public class DAGBuilder {

  Multimap<PipelineNode, PipelineNode> dagInputs = HashMultimap.create();
  Map<ObjectIdentifier, PipelineNode> nodeLookup = new HashMap<>();
  Map<NamePath, AnnotatedSqrlTableFunction> functions = new HashMap<>();




}
