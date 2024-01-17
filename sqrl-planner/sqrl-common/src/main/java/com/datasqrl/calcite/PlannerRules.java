package com.datasqrl.calcite;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRules;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.DateRangeRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.materialize.MaterializedViewRules;
import org.apache.calcite.rel.stream.StreamRules;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.calcite.shaded.com.google.common.collect.UnmodifiableIterator;

import java.util.List;

public class PlannerRules {

    private static final List<RelOptRule> CALC_RULES;
    private static final List<RelOptRule> BASE_RULES;
    private static final List<RelOptRule> ABSTRACT_RULES;
    private static final List<RelOptRule> ABSTRACT_RELATIONAL_RULES;
    private static final List<RelOptRule> CONSTANT_REDUCTION_RULES;
    private static final List<RelOptRule> MATERIALIZATION_RULES;

    static {
        CALC_RULES = ImmutableList.of(
                Bindables.FROM_NONE_RULE,
                EnumerableRules.ENUMERABLE_CALC_RULE,
                EnumerableRules.ENUMERABLE_FILTER_TO_CALC_RULE,
                EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE,
                CoreRules.CALC_MERGE,
                CoreRules.FILTER_CALC_MERGE,
                CoreRules.PROJECT_CALC_MERGE,
                CoreRules.FILTER_TO_CALC,
                CoreRules.PROJECT_TO_CALC,
                CoreRules.CALC_MERGE,
                CoreRules.FILTER_CALC_MERGE,
                CoreRules.PROJECT_CALC_MERGE,
                new RelOptRule[0]);
        BASE_RULES = ImmutableList.of(CoreRules.AGGREGATE_STAR_TABLE,
                CoreRules.AGGREGATE_PROJECT_STAR_TABLE,
                CoreRules.PROJECT_MERGE,
                CoreRules.FILTER_SCAN,
                CoreRules.PROJECT_FILTER_TRANSPOSE,
//                CoreRules.FILTER_PROJECT_TRANSPOSE, // exclude as a default rule due to infinite loop when converting to bindable SQRL-#479.
                CoreRules.FILTER_INTO_JOIN,
                CoreRules.JOIN_PUSH_EXPRESSIONS,
                CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
                CoreRules.AGGREGATE_CASE_TO_FILTER,
                CoreRules.AGGREGATE_REDUCE_FUNCTIONS,
                CoreRules.FILTER_AGGREGATE_TRANSPOSE,
                CoreRules.PROJECT_WINDOW_TRANSPOSE,
                CoreRules.MATCH,
                CoreRules.JOIN_COMMUTE,
                JoinPushThroughJoinRule.RIGHT,
                JoinPushThroughJoinRule.LEFT,
                CoreRules.SORT_PROJECT_TRANSPOSE,
                CoreRules.SORT_JOIN_TRANSPOSE,
                CoreRules.SORT_REMOVE_CONSTANT_KEYS,
                CoreRules.SORT_UNION_TRANSPOSE,
                CoreRules.EXCHANGE_REMOVE_CONSTANT_KEYS,
                CoreRules.SORT_EXCHANGE_REMOVE_CONSTANT_KEYS);
        ABSTRACT_RULES = ImmutableList.of(CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS,
                CoreRules.UNION_PULL_UP_CONSTANTS,
                PruneEmptyRules.UNION_INSTANCE,
                PruneEmptyRules.INTERSECT_INSTANCE,
                PruneEmptyRules.MINUS_INSTANCE,
                PruneEmptyRules.PROJECT_INSTANCE,
                PruneEmptyRules.FILTER_INSTANCE,
                PruneEmptyRules.SORT_INSTANCE,
                PruneEmptyRules.AGGREGATE_INSTANCE,
                PruneEmptyRules.JOIN_LEFT_INSTANCE,
                PruneEmptyRules.JOIN_RIGHT_INSTANCE,
                PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE,
                CoreRules.UNION_MERGE,
                CoreRules.INTERSECT_MERGE,
                CoreRules.MINUS_MERGE,
                CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW,
                CoreRules.FILTER_MERGE,
                DateRangeRules.FILTER_INSTANCE,
                CoreRules.INTERSECT_TO_DISTINCT);
        ABSTRACT_RELATIONAL_RULES = ImmutableList.of(CoreRules.FILTER_INTO_JOIN,
                CoreRules.JOIN_CONDITION_PUSH,
                AbstractConverter.ExpandConversionRule.INSTANCE,
                CoreRules.JOIN_COMMUTE,
                CoreRules.PROJECT_TO_SEMI_JOIN,
                CoreRules.JOIN_TO_SEMI_JOIN,
                CoreRules.AGGREGATE_REMOVE,
                CoreRules.UNION_TO_DISTINCT,
                CoreRules.PROJECT_REMOVE,
                CoreRules.PROJECT_AGGREGATE_MERGE,
                CoreRules.AGGREGATE_JOIN_TRANSPOSE,
                CoreRules.AGGREGATE_MERGE,
                new RelOptRule[]{CoreRules.AGGREGATE_PROJECT_MERGE,
                        CoreRules.CALC_REMOVE,
                        CoreRules.SORT_REMOVE});
        CONSTANT_REDUCTION_RULES = ImmutableList.of(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
                CoreRules.FILTER_REDUCE_EXPRESSIONS,
                CoreRules.CALC_REDUCE_EXPRESSIONS,
                CoreRules.WINDOW_REDUCE_EXPRESSIONS,
                CoreRules.JOIN_REDUCE_EXPRESSIONS,
                CoreRules.FILTER_VALUES_MERGE,
                CoreRules.PROJECT_FILTER_VALUES_MERGE,
                CoreRules.PROJECT_VALUES_MERGE,
                CoreRules.AGGREGATE_VALUES);
        MATERIALIZATION_RULES = ImmutableList.of(MaterializedViewRules.FILTER_SCAN,
                MaterializedViewRules.PROJECT_FILTER,
                MaterializedViewRules.FILTER,
                MaterializedViewRules.PROJECT_JOIN,
                MaterializedViewRules.JOIN,
                MaterializedViewRules.PROJECT_AGGREGATE,
                MaterializedViewRules.AGGREGATE);
    }

    public static void registerDefaultRules(RelOptPlanner planner, boolean enableMaterializations, boolean enableBindable) {
        if (CalciteSystemProperty.ENABLE_COLLATION_TRAIT.value()) {
            registerAbstractRelationalRules(planner);
        }

        registerAbstractRules(planner);
        registerBaseRules(planner);
        if (enableMaterializations) {
            registerMaterializationRules(planner);
        }

        UnmodifiableIterator var3;
        RelOptRule rule;
        if (enableBindable) {
            var3 = Bindables.RULES.iterator();

            while (var3.hasNext()) {
                rule = (RelOptRule) var3.next();
                planner.addRule(rule);
            }
        }

        planner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
        planner.addRule(CoreRules.PROJECT_TABLE_SCAN);
        planner.addRule(CoreRules.PROJECT_INTERPRETER_TABLE_SCAN);
        if (CalciteSystemProperty.ENABLE_ENUMERABLE.value()) {
            registerEnumerableRules(planner);
            planner.addRule(EnumerableRules.TO_INTERPRETER);
        }

        if (enableBindable && CalciteSystemProperty.ENABLE_ENUMERABLE.value()) {
            planner.addRule(EnumerableRules.TO_BINDABLE);
        }

        if (CalciteSystemProperty.ENABLE_STREAM.value()) {
            var3 = StreamRules.RULES.iterator();

            while (var3.hasNext()) {
                rule = (RelOptRule) var3.next();
                planner.addRule(rule);
            }
        }

        planner.addRule(CoreRules.FILTER_REDUCE_EXPRESSIONS);
    }

    public static void registerAbstractRules(RelOptPlanner planner) {
        ABSTRACT_RULES.forEach(planner::addRule);
    }

    public static void registerAbstractRelationalRules(RelOptPlanner planner) {
        ABSTRACT_RELATIONAL_RULES.forEach(planner::addRule);
    }

    private static void registerEnumerableRules(RelOptPlanner planner) {
        EnumerableRules.ENUMERABLE_RULES.forEach(planner::addRule);
    }

    private static void registerBaseRules(RelOptPlanner planner) {
        BASE_RULES.forEach(planner::addRule);
    }

    private static void registerReductionRules(RelOptPlanner planner) {
        CONSTANT_REDUCTION_RULES.forEach(planner::addRule);
    }

    private static void registerMaterializationRules(RelOptPlanner planner) {
        RelOptRules.MATERIALIZATION_RULES.forEach(planner::addRule);
    }

    private static void registerCalcRules(RelOptPlanner planner) {
        RelOptRules.CALC_RULES.forEach(planner::addRule);
    }
}
