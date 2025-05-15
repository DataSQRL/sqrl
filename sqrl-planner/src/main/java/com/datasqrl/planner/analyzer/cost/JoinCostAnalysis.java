/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.planner.analyzer.cost;

import com.datasqrl.io.tables.TableType;
import com.datasqrl.plan.rules.Side;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class JoinCostAnalysis implements CostAnalysis {

  final TableType leftType;
  final TableType rightType;
  /**
   * The number of equality constraints between the two sides of the join
   * to estimate cardinality of the result
   */
  final int numEqualities;
  /**
   * The side which has (at most) a single row matching any given row on the
   * other side because of a pk constraint.
   */
  final Side singletonSide;


  @Override
  public double getCostMultiplier() {
    var localCost = 0.0;
    if (getSingletonSide()!= Side.LEFT) {
      localCost += perSideCost(getLeftType());
    }
    if (getSingletonSide()!= Side.RIGHT) {
      localCost += perSideCost(getRightType());
    }
    if (getSingletonSide()==Side.NONE && getNumEqualities() == 0) {
      localCost *= 100;
    }
    assert localCost >= 1;
    return localCost;
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


}
