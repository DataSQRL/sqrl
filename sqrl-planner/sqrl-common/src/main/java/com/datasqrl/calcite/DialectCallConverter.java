package com.datasqrl.calcite;

import com.datasqrl.calcite.function.RuleTransform;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.tools.Programs;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DialectCallConverter {
  private final RelOptPlanner planner;

  public DialectCallConverter(RelOptPlanner planner) {
    this.planner = planner;
  }

  public RelNode convert(Dialect dialect, RelNode relNode) {
    List<RuleTransform> transforms = extractFunctionTransforms(relNode);

    List<RelRule> rules = new ArrayList<>();
    for (RuleTransform transform : transforms) {
      rules.addAll(transform.transform(dialect, (SqlOperator) transform));
    }

    relNode = Programs.hep(rules, false, null)
        .run(planner, relNode, relNode.getTraitSet(),
            List.of(), List.of());
    return relNode;
  }

  private List<RuleTransform> extractFunctionTransforms(RelNode relNode) {
    Set<RuleTransform> transforms = new HashSet<>();
    relNode.accept(new RelShuttleImpl() {
      @Override
      protected RelNode visitChild(RelNode parent, int i, RelNode child) {
        parent.accept(new RexShuttle(){
          @Override
          public RexNode visitCall(RexCall call) {
            if (call.getOperator() instanceof RuleTransform) {
              transforms.add((RuleTransform)call.getOperator());
            }
            return super.visitCall(call);
          }
        });

        return super.visitChild(parent, i, child);
      }
    });

    return new ArrayList<>(transforms);
  }
}
