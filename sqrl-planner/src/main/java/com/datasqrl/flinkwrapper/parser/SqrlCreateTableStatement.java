package com.datasqrl.flinkwrapper.parser;

import com.datasqrl.flinkwrapper.SqrlEnvironment;
import lombok.Value;

@Value
public class SqrlCreateTableStatement implements SqrlStatement {

  ParsedObject<String> createTable;

  @Override
  public void apply(SqrlEnvironment sqrlEnv) {
    throw new UnsupportedOperationException();
  }
}
