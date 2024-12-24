package com.datasqrl.flinkwrapper.parser;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.flinkwrapper.SqrlEnvironment;
import lombok.Value;
import org.apache.calcite.sql.SqlIdentifier;

@Value
public class SqrlExportStatement implements SqrlStatement {

  ParsedObject<NamePath> tableIdentifier;
  ParsedObject<NamePath> packageIdentifier;

  @Override
  public void apply(SqrlEnvironment sqrlEnv) {
    throw new UnsupportedOperationException();
  }

}
