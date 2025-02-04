package com.datasqrl.v2.parser;

import lombok.Value;
import org.apache.calcite.sql.SqlNode;

@Value
public class ParsedSql {

  SqlNode sqlNode;
  String originalSql;

}
