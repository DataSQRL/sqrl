package com.datasqrl.calcite;

import com.datasqrl.calcite.function.RuleTransform;
import com.datasqrl.function.FunctionTranslationMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
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
    Map<SqlOperator, RuleTransform> transforms = extractFunctionTransforms(relNode);

    List<RelRule> rules = new ArrayList<>();
    for (Entry<SqlOperator, RuleTransform> transform : transforms.entrySet()) {
      rules.addAll(transform.getValue().transform(dialect, transform.getKey()));
    }

    relNode = Programs.hep(rules, false, null)
        .run(planner, relNode, relNode.getTraitSet(),
            List.of(), List.of());
    return relNode;
  }

  private Map<SqlOperator, RuleTransform> extractFunctionTransforms(RelNode relNode) {
    Map<SqlOperator, RuleTransform> transforms = new HashMap<>();
    relNode.accept(new RelShuttleImpl() {
      @Override
      protected RelNode visitChild(RelNode parent, int i, RelNode child) {
        parent.accept(new RexShuttle(){
          @Override
          public RexNode visitCall(RexCall call) {
            RuleTransform transform = FunctionTranslationMap.transformMap.get(
                call.getOperator().getName().toLowerCase());
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
