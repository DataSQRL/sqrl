package com.datasqrl.parse;

import com.datasqrl.error.ErrorCollector;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqrlStatement;

public interface SqrlParser {

  ScriptNode parse(String sql, ErrorCollector errors);

  SqrlStatement parseStatement(String sql);
}
