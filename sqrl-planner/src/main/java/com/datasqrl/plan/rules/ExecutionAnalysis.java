/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.rules;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlOperator;

import com.datasqrl.calcite.SqrlRexUtil;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.table.PhysicalRelationalTable;

import lombok.Getter;
import lombok.NonNull;
import lombok.Value;

@Value
public class ExecutionAnalysis {

  @NonNull ExecutionStage stage;


  public static ExecutionAnalysis of(@NonNull ExecutionStage stage) {
    return new ExecutionAnalysis(stage);
  }

  public void requireFeature(EngineFeature... requiredCapabilities) {
    requireFeature(Arrays.asList(requiredCapabilities));
  }

  public boolean isMaterialize(PhysicalRelationalTable sourceTable) {
    //check if this is a stage transition and the result stage supports materialization on key
    return sourceTable.getAssignedStage().map(s -> !s.equals(stage)).orElse(false)
        && supportsFeature(EngineFeature.MATERIALIZE_ON_KEY);
  }

  public boolean supportsFeature(EngineFeature... capabilities) {
    return stage.supportsAllFeatures(Arrays.asList(capabilities));
  }

  public void requireFeature(Collection<EngineFeature> requiredCapabilities) {
    if (!stage.supportsAllFeatures(requiredCapabilities)) {
      throw new CapabilityException(stage, requiredCapabilities.stream().map(EngineCapability.Feature::new).collect(Collectors.toUnmodifiableList()));
    }
  }

  @Getter
  public static class CapabilityException extends RuntimeException {

    private final ExecutionStage stage;
    private final Collection<EngineCapability> capabilities;

    public CapabilityException(ExecutionStage stage, Collection<EngineCapability> capabilities) {
      super(String.format("Execution stage [%s] does not support capabilities [%s].",
          stage.getName(), capabilities));
      this.capabilities = capabilities;
      this.stage = stage;
    }

  }


  public void requireRex(Iterable<RexNode> nodes) {
    var rexAnalysis = new RexCapabilityAnalysis();
    nodes.forEach(rex -> rex.accept(rexAnalysis));
    List<EngineCapability> missingCapabilities = new ArrayList<>();
    rexAnalysis.capabilities.stream().filter(Predicate.not(this::supportsFeature))
            .map(EngineCapability.Feature::new).forEach(missingCapabilities::add);
    rexAnalysis.functions.stream().filter(Predicate.not(stage::supportsFunction))
            .map(EngineCapability.Function::new).forEach(missingCapabilities::add);
    if (!missingCapabilities.isEmpty()) {
      throw new CapabilityException(stage, missingCapabilities);
    }
  }

  public void requireRex(RexNode node) {
    this.requireRex(List.of(node));
  }

  public void requireAggregates(Iterable<AggregateCall> aggregates) {
    //TODO: implement once we have non-SQL aggregate functions
  }

  public static class RexCapabilityAnalysis extends RexVisitorImpl<Void> {

    private final EnumSet<EngineFeature> capabilities = EnumSet.noneOf(EngineFeature.class);
    private final Set<SqlOperator> functions = new HashSet();

    public RexCapabilityAnalysis() {
      super(true);
    }

    @Override
    public Void visitCall(RexCall call) {
      if (SqrlRexUtil.isNOW(call.getOperator())) {
        capabilities.add(EngineFeature.NOW);
      } else {
        functions.add(call.getOperator());
      }
      return super.visitCall(call);
    }
  }

}
