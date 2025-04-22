/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.rules;

import java.util.Optional;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Join;

import com.datasqrl.engine.database.AnalyticDatabaseEngine;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.plan.hints.JoinCostHint;
import com.datasqrl.plan.hints.SqrlHint;
import com.datasqrl.plan.rules.JoinAnalysis.Side;
import com.google.common.base.Preconditions;

import lombok.NonNull;
import lombok.Value;

@Value
public
class SimpleCostModel implements ComputeCost {

  private final double cost;

  private SimpleCostModel(double cost) {
    this.cost = cost;
  }

  public static SimpleCostModel of(ExecutionStage executionStage, RelNode relNode) {
    var cost = 1.0;
    switch (executionStage.getEngine().getType()) {
      case DATABASE:
        //Currently we make the simplifying assumption that database execution is the baseline and we compare
        //other engines against it
        //However, if the database is a table format, we apply a penalty because query engines are less efficient.
        if (executionStage.getEngine() instanceof AnalyticDatabaseEngine) {
          cost = cost * 1.3;
        }
        break;
      case STREAMS:
        //We assume that pre-computing is generally cheaper (by factor of 10) unless (standard) joins are
        //involved which can lead to combinatorial explosion. So, we primarily cost the joins
        cost = joinCost(relNode);
        cost = cost / 10;
        break;
      case SERVER:
        cost = cost * 2;
        break;
//      case STATIC:
//        return 1;
      default:
        throw new UnsupportedOperationException("Unsupported engine type: " + executionStage.getEngine().getType());
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
            var localCost = 0.0;
            var jch = costHintOpt.get();
            if (jch.getSingletonSide()!= Side.LEFT) {
              localCost += perSideCost(jch.getLeftType());
            }
            if (jch.getSingletonSide()!= Side.RIGHT) {
              localCost += perSideCost(jch.getRightType());
            }
            if (jch.getSingletonSide()==Side.NONE && jch.getNumEqualities() == 0) {
              localCost *= 100;
            }
            assert localCost >= 1;
            joinCost *= localCost;
          }
        }
        super.visit(node, ordinal, parent);
      }

      private double perSideCost(TableType tableType) {
        return switch (tableType) {
        case STREAM -> 100;
        case STATE -> 10;
        case VERSIONED_STATE -> 4;
        case STATIC -> 1;
        case LOOKUP -> 2;
        case RELATION -> 20;
        default -> throw new UnsupportedOperationException();
        };
      }

      double run(RelNode node) {
        go(node);
        return joinCost;
      }
    }

    return new JoinCounter().run(rootRel);
  }
}
