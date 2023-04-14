package com.datasqrl.frontend;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.parse.SqrlParser;
import com.google.inject.Inject;
import org.apache.calcite.sql.ScriptNode;

public class SqrlParse extends SqrlBase {

  protected SqrlParser parser;

  @Inject
  public SqrlParse(SqrlParser parser, ErrorCollector errors) {
    super(errors);
    this.parser = parser;
  }

  public ScriptNode parse(String sqrl) {
    return parser.parse(sqrl, errors);
  }
}
