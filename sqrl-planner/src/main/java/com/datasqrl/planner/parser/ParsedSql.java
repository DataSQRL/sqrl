package com.datasqrl.planner.parser;

import org.apache.calcite.sql.SqlNode;

import lombok.Value;

@Value
public class ParsedSql {

  SqlNode sqlNode;
  String originalSql;

}
