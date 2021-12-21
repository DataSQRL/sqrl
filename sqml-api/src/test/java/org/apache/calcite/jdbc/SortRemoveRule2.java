package org.apache.calcite.jdbc;

import java.util.List;
import java.util.Set;
import org.apache.calcite.jdbc.SortRemoveRule2.Config;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

public class SortRemoveRule2
      extends RelRule<Config>
      implements TransformationRule {

    /** Creates a SortRemoveRule. */
    protected SortRemoveRule2(Config config) {
      super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      System.out.println();
      //TODO: Maybe add a trait to the context table?
//      if (!call.getPlanner().getRelTraitDefs()
//          .contains(RelCollationTraitDef.INSTANCE)) {
//        // Collation is not an active trait.
//        return;
//      }
//
//      final RexBuilder rexBuilder = call.rel(0).getCluster().getRexBuilder();
//
//      LogicalJoin join = LogicalJoin.create(scan, scan, List.of(),
//          rexBuilder.makeLiteral(true), Set.of(), JoinRelType.INNER);

//      final RelCollation collation = sort.getCollation();
//      assert collation == sort.getTraitSet()
//          .getTrait(RelCollationTraitDef.INSTANCE);
//      final RelTraitSet traits = sort.getInput().getTraitSet()
//          .replace(collation).replace(sort.getConvention());
      call.transformTo(call.rel(0).getInput(0));
//
    }

    /** Rule configuration. */
    public interface Config extends RelRule.Config {
      SortRemoveRule2.Config DEFAULT = EMPTY
          .withOperandSupplier(b ->
              b.operand(LogicalSort.class).anyInputs())
          .as(SortRemoveRule2.Config.class);

      @Override default SortRemoveRule2 toRule() {
//        return null;
        return new SortRemoveRule2(this);
      }
    }
  }