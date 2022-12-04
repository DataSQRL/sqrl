package com.datasqrl.util;

import org.apache.calcite.schema.Statistic;

public class CalciteWriter {

  public static String toString(Statistic stats) {
    StringBuilder s = new StringBuilder();
    s.append("#").append(Math.round(stats.getRowCount()));
    s.append("-idx:").append(stats.getKeys());
    return s.toString();
  }

}
