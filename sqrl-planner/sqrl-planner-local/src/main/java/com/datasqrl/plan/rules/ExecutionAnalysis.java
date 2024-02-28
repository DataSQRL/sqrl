/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.rules;

import com.datasqrl.engine.EngineCapability;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.util.FunctionUtil;
import com.datasqrl.util.SqrlRexUtil;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;

@Value
public class ExecutionAnalysis {

  @NonNull ExecutionStage stage;


  public static ExecutionAnalysis of(@NonNull ExecutionStage stage) {
    return new ExecutionAnalysis(stage);
  }

  public void require(EngineCapability... requiredCapabilities) {
    require(Arrays.asList(requiredCapabilities));
  }

  public boolean isMaterialize(PhysicalRelationalTable sourceTable) {
    //check if this is a stage transition and the result stage supports materialization on key
    return sourceTable.getAssignedStage().map(s -> !s.equals(stage)).orElse(false)
        && supports(EngineCapability.MATERIALIZE_ON_KEY);
  }

  public boolean supports(EngineCapability... capabilities) {
    return stage.supportsAll(Arrays.asList(capabilities));
  }

  public void require(Collection<EngineCapability> requiredCapabilities) {
    if (!stage.supportsAll(requiredCapabilities)) {
      throw new CapabilityException(stage, requiredCapabilities);
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
    RexCapabilityAnalysis rexAnalysis = new RexCapabilityAnalysis();
    nodes.forEach(rex -> rex.accept(rexAnalysis));
    require(rexAnalysis.capabilities);
  }

  public void requireRex(RexNode node) {
    this.requireRex(List.of(node));
  }

  public void requireAggregates(Iterable<AggregateCall> aggregates) {
    //TODO: implement once we have non-SQL aggregate functions
  }

  public static class RexCapabilityAnalysis extends RexVisitorImpl<Void> {

    private final EnumSet<EngineCapability> capabilities = EnumSet.noneOf(EngineCapability.class);

    public RexCapabilityAnalysis() {
      super(true);
    }

    @Override
    public Void visitCall(RexCall call) {
      if (SqrlRexUtil.isNOW(call.getOperator())) {
        capabilities.add(EngineCapability.NOW);
      } else {
        FunctionUtil.getSqrlFunction(call.getOperator())
            .flatMap(FunctionUtil::getTimestampPreservingFunction)
            .ifPresent(f->capabilities.add(EngineCapability.EXTENDED_FUNCTIONS));
      }
      return super.visitCall(call);
    }
  }

}
