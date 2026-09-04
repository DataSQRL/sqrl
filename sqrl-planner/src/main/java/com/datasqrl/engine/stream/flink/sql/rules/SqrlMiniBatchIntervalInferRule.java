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
package com.datasqrl.engine.stream.flink.sql.rules;

import java.util.List;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDataStreamScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLegacyTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMiniBatchAssigner;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.rules.physical.stream.MiniBatchIntervalInferRule;
import org.apache.flink.table.planner.plan.trait.MiniBatchInterval;
import org.apache.flink.table.planner.plan.trait.MiniBatchIntervalTrait;
import org.apache.flink.table.planner.plan.trait.MiniBatchIntervalTraitDef;
import org.apache.flink.table.planner.plan.trait.MiniBatchMode;
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.immutables.value.Value;

/**
 * Drop-in replacement for Flink's {@link MiniBatchIntervalInferRule} that compares mini-batch
 * traits by value instead of identity, so inputs whose interval is unchanged are not copied. Flink
 * builds a fresh trait per match, which makes the identity check fire on every watermark-requiring
 * operator and costs the HepPlanner a full-graph garbage collection per no-op transformation.
 */
public class SqrlMiniBatchIntervalInferRule
    extends RelRule<SqrlMiniBatchIntervalInferRule.SqrlMiniBatchIntervalInferRuleConfig> {

  public static final SqrlMiniBatchIntervalInferRule INSTANCE =
      SqrlMiniBatchIntervalInferRuleConfig.DEFAULT.toRule();

  protected SqrlMiniBatchIntervalInferRule(SqrlMiniBatchIntervalInferRuleConfig config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    StreamPhysicalRel rel = call.rel(0);
    var miniBatchIntervalTrait = rel.getTraitSet().getTrait(MiniBatchIntervalTraitDef.INSTANCE());
    var inputs = getInputs(rel);
    var updatedTrait = getUpdatedTrait(rel, miniBatchIntervalTrait);
    var updatedInputs = inputs.stream().map(input -> getUpdatedInput(input, updatedTrait)).toList();

    if (!inputs.equals(updatedInputs)) {
      call.transformTo(rel.copy(rel.getTraitSet(), updatedInputs));
    }
  }

  private MiniBatchIntervalTrait getUpdatedTrait(
      StreamPhysicalRel rel, MiniBatchIntervalTrait miniBatchIntervalTrait) {
    if (rel instanceof StreamPhysicalGroupWindowAggregate) {
      return MiniBatchIntervalTrait.NO_MINIBATCH();
    }
    if (rel instanceof StreamPhysicalWatermarkAssigner
        || rel instanceof StreamPhysicalMiniBatchAssigner) {
      return MiniBatchIntervalTrait.NONE();
    }

    var tableConfig = ShortcutUtils.unwrapTableConfig(rel);
    var miniBatchEnabled = tableConfig.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED);
    if (rel.requireWatermark() && miniBatchEnabled) {
      var mergedInterval =
          FlinkRelOptUtil.mergeMiniBatchInterval(
              miniBatchIntervalTrait.getMiniBatchInterval(),
              new MiniBatchInterval(0, MiniBatchMode.RowTime));
      return new MiniBatchIntervalTrait(mergedInterval);
    }
    return miniBatchIntervalTrait;
  }

  private RelNode getUpdatedInput(RelNode input, MiniBatchIntervalTrait updatedTrait) {
    if (shouldAppendMiniBatchAssignerNode(input)) {
      return new StreamPhysicalMiniBatchAssigner(
          input.getCluster(),
          input.getTraitSet(),
          input.copy(input.getTraitSet().plus(MiniBatchIntervalTrait.NONE()), input.getInputs()));
    }

    var originTrait = input.getTraitSet().getTrait(MiniBatchIntervalTraitDef.INSTANCE());
    if (originTrait.equals(updatedTrait)) {
      return input;
    }

    var inferredTrait =
        new MiniBatchIntervalTrait(
            FlinkRelOptUtil.mergeMiniBatchInterval(
                originTrait.getMiniBatchInterval(), updatedTrait.getMiniBatchInterval()));
    if (inferredTrait.equals(originTrait)) {
      return input;
    }
    return input.copy(input.getTraitSet().plus(inferredTrait), input.getInputs());
  }

  private List<RelNode> getInputs(RelNode parent) {
    return parent.getInputs().stream().map(i -> ((HepRelVertex) i).getCurrentRel()).toList();
  }

  private boolean shouldAppendMiniBatchAssignerNode(RelNode node) {
    var mode =
        node.getTraitSet()
            .getTrait(MiniBatchIntervalTraitDef.INSTANCE())
            .getMiniBatchInterval()
            .getMode();
    if (node instanceof StreamPhysicalDataStreamScan
        || node instanceof StreamPhysicalLegacyTableSourceScan
        || node instanceof StreamPhysicalTableSourceScan
        || node instanceof StreamPhysicalWatermarkAssigner) {
      return mode == MiniBatchMode.RowTime || mode == MiniBatchMode.ProcTime;
    }
    return false;
  }

  @Value.Immutable
  public interface SqrlMiniBatchIntervalInferRuleConfig extends RelRule.Config {
    SqrlMiniBatchIntervalInferRuleConfig DEFAULT =
        ImmutableSqrlMiniBatchIntervalInferRuleConfig.builder()
            .description("SqrlMiniBatchIntervalInferRule")
            .operandSupplier(b0 -> b0.operand(StreamPhysicalRel.class).anyInputs())
            .build();

    @Override
    default SqrlMiniBatchIntervalInferRule toRule() {
      return new SqrlMiniBatchIntervalInferRule(this);
    }
  }
}
