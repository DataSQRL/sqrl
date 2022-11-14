package ai.datasqrl.plan.calcite.rules;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalJoin;

import java.util.ArrayList;
import java.util.List;

/**
 * Additional join rule for Enumerable to make sure that NestedLoopJoin is scored
 * in the cost model since the EnumerableJoinRule will only offer HashJoin as an option
 * for joins with equi-conditions.
 *
 * Copied from Calcite's EnumerableJoinRule
 */
public class EnumerableNestedLoopJoinRule extends ConverterRule {

    /** Default configuration. */
    public static final ConverterRule.Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalJoin.class, Convention.NONE,
                    EnumerableConvention.INSTANCE, "EnumerableNestedLoopJoinRule")
            .withRuleFactory(EnumerableNestedLoopJoinRule::new);

    public static final RelOptRule INSTANCE =
            EnumerableNestedLoopJoinRule.DEFAULT_CONFIG.toRule(EnumerableNestedLoopJoinRule.class);

    /** Called from the Config. */
    protected EnumerableNestedLoopJoinRule(Config config) {
        super(config);
    }

    @Override public RelNode convert(RelNode rel) {
        LogicalJoin join = (LogicalJoin) rel;
        List<RelNode> newInputs = new ArrayList<>();
        for (RelNode input : join.getInputs()) {
            if (!(input.getConvention() instanceof EnumerableConvention)) {
                input =
                        convert(
                                input,
                                input.getTraitSet()
                                        .replace(EnumerableConvention.INSTANCE));
            }
            newInputs.add(input);
        }
        final RelNode left = newInputs.get(0);
        final RelNode right = newInputs.get(1);

        return EnumerableNestedLoopJoin.create(
                left,
                right,
                join.getCondition(),
                join.getVariablesSet(),
                join.getJoinType());
    }

}
