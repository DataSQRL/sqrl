package com.datasqrl.calcite;

import com.datasqrl.calcite.function.OperatorRuleTransform;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.util.ServiceLoaderDiscovery;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.tools.Programs;

/**
 * Converts functions to the given dialect via rule transformations.
 * Rule transformations are implemented as {@link OperatorRuleTransform} for "structural" transformations
 * that have to happen at the {@link RelNode} level.
 *
 * Simpler transformations that only require switching out the function name or parameter order
 * should be implemented via {@link com.datasqrl.function.translations.SqlTranslation} which happen
 * during unparsing.
 */
public class DialectCallConverter {
  private final RelOptPlanner planner;

  public static final Map<Name, OperatorRuleTransform> transformMap = ServiceLoaderDiscovery.getAll(
          OperatorRuleTransform.class)
      .stream().collect(Collectors.toMap(t->Name.system(t.getRuleOperatorName()), t->t));

  public DialectCallConverter(RelOptPlanner planner) {
    this.planner = planner;
  }

  public RelNode convert(Dialect dialect, RelNode relNode) {
    Map<SqlOperator, OperatorRuleTransform> transforms = extractFunctionTransforms(relNode);

    List<RelRule> rules = new ArrayList<>();
    for (Entry<SqlOperator, OperatorRuleTransform> transform : transforms.entrySet()) {
      rules.addAll(transform.getValue().transform(dialect, transform.getKey()));
    }

    relNode = Programs.hep(rules, false, null)
        .run(planner, relNode, relNode.getTraitSet(),
            List.of(), List.of());

    return relNode;
  }

  private Map<SqlOperator, OperatorRuleTransform> extractFunctionTransforms(RelNode relNode) {
    Map<SqlOperator, OperatorRuleTransform> transforms = new HashMap<>();
    relNode.accept(new RelShuttleImpl() {

      @Override
      public RelNode visit(LogicalAggregate aggregate) {
        for (AggregateCall call : aggregate.getAggCallList()) {
          OperatorRuleTransform transform = transformMap.get(
              Name.system(call.getAggregation().getName()));
          if (transform != null) {
            transforms.put(call.getAggregation(), transform);
          }

        }
        return super.visit(aggregate);
      }

      @Override
      protected RelNode visitChild(RelNode parent, int i, RelNode child) {
        parent.accept(new RexShuttle(){
          @Override
          public RexNode visitCall(RexCall call) {
            OperatorRuleTransform transform = transformMap.get(
                Name.system(call.getOperator().getName()));
            if (transform != null) {
              transforms.put(call.getOperator(), transform);
            }
            return super.visitCall(call);
          }
        });

        return super.visitChild(parent, i, child);
      }
    });

    return new HashMap<>(transforms);
  }
}
