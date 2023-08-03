/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.rules;

import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.plan.hints.JoinCostHint;
import com.datasqrl.plan.hints.SqrlHint;
import com.datasqrl.plan.table.TableType;
import com.google.common.base.Preconditions;
import java.util.Optional;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Join;

@Value
public
class SimpleCostModel implements ComputeCost {

  private final double cost;

  private SimpleCostModel(double cost) {
    this.cost = cost;
  }

  public static SimpleCostModel of(ExecutionEngine.Type engineType, RelNode relNode) {
    double cost = 1.0;
    switch (engineType) {
      case DATABASE:
        //Currently we make the simplifying assumption that database execution is the baseline and we compare
        //other engines against it
        break;
      case STREAM:
        //We assume that pre-computing is generally cheaper (by factor of 10) unless (standard) joins are
        //involved which can lead to combinatorial explosion. So, we primarily cost the joins
        cost = joinCost(relNode);
        cost = cost / 10;
        break;
      case SERVER:
        cost = cost * 2;
        break;
      default:
        throw new UnsupportedOperationException("Unsupported engine type: " + engineType);
    }
    return new SimpleCostModel(cost);
  }

  @Override
  public int compareTo(@NonNull ComputeCost o) {
    Preconditions.checkArgument(o instanceof SimpleCostModel);
    return Double.compare(cost, ((SimpleCostModel) o).cost);
  }

  public static double joinCost(RelNode rootRel) {
    /** Visitor that counts join nodes. */
    class JoinCounter extends RelVisitor {

      double joinCost = 1.0;

      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node instanceof Join) {
          Optional<JoinCostHint> costHintOpt = SqrlHint.fromRel(node, JoinCostHint.CONSTRUCTOR);
          if (costHintOpt.isPresent()) {
            double localCost = 0.0;
            JoinCostHint jch = costHintOpt.get();
            localCost += perSideCost(jch.getLeftType());
            localCost += perSideCost(jch.getRightType());
            if (jch.getNumEqualities() == 0) {
              localCost *= 100;
            }
            assert localCost >= 1;
            joinCost *= localCost;
          }
        }
        super.visit(node, ordinal, parent);
      }

      private double perSideCost(TableType tableType) {
        switch (tableType) {
          case STREAM:
            return 100;
          case STATE:
            return 10;
          case DEDUP_STREAM:
            return 4;
          default:
            throw new UnsupportedOperationException();
        }
      }

      double run(RelNode node) {
        go(node);
        return joinCost;
      }
    }

    return new JoinCounter().run(rootRel);
  }
}
