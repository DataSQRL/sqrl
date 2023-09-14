package com.datasqrl.parse;

import org.apache.calcite.sql.Symbolizable;

public enum SqlDistinctKeyword implements Symbolizable {
  DISTINCT_ON;

  private SqlDistinctKeyword() {
  }
}
