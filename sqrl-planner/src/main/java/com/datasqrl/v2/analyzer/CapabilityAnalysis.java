package com.datasqrl.v2.analyzer;

import com.datasqrl.calcite.SqrlRexUtil;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.plan.rules.EngineCapability;
import com.datasqrl.plan.rules.EngineCapability.Feature;
import com.datasqrl.plan.rules.EngineCapability.Function;
import java.util.HashSet;
import java.util.Set;
import lombok.Getter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlOperator;

/**
 * Analyzes the engine capabilities needed to execute a particular query
 */
public class CapabilityAnalysis extends RexVisitorImpl<Void> {

  @Getter
  private final Set<EngineCapability> requiredCapabilities = new HashSet<>();

  public CapabilityAnalysis() {
    super(true);
  }

  public void add(EngineFeature feature) {
    requiredCapabilities.add(new Feature(feature));
  }

  public void add(SqlOperator operator) {
    requiredCapabilities.add(new Function(operator));
  }

  @Override
  public Void visitCall(RexCall call) {
    if (SqrlRexUtil.isNOW(call.getOperator())) {
      add(EngineFeature.NOW);
    } else {
      add(call.getOperator());
    }
    return super.visitCall(call);
  }

  public void analyzeRexNode(Iterable<RexNode> rexNodes) {
    rexNodes.forEach(rexNode -> rexNode.accept(this));
  }

  public void analyzeRexNode(RexNode rexNode) {
    rexNode.accept(this);
  }

  public void analyzeAggregates(Iterable<AggregateCall> aggregates) {
    //TODO: implement once we have non-SQL aggregate functions
  }
}
