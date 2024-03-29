/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import lombok.Value;

@Value
public class TableStatistic {

  private static final double DEFAULT_ROW_COUNT = 1e15;

  public static final TableStatistic UNKNOWN = new TableStatistic(Double.NaN);
  public static final double DEFAULT_NESTED_MULTIPLIER = 2.0;

  private final double rowCount;

  public static TableStatistic of(double rowCount) {
    return new TableStatistic(rowCount);
  }

  @Override
  public String toString() {
    return "Stats=" + rowCount;
  }

  public boolean isUnknown() {
    return Double.isNaN(rowCount);
  }

  public TableStatistic nested() {
    return nested(DEFAULT_NESTED_MULTIPLIER);
  }

  public TableStatistic nested(double multiplier) {
    if (isUnknown()) {
      return UNKNOWN;
    }
    return new TableStatistic(rowCount * multiplier);
  }

  public double getRowCount() {
    if (isUnknown()) {
      return DEFAULT_ROW_COUNT;
    } else {
      return rowCount;
    }
  }

}
