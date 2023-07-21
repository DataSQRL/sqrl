/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.parse;

import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorLocation;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.error.ErrorLocation.FileRange;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
@Slf4j
public class SqrlAstException extends RuntimeException {

  private final ErrorLabel errorLabel;
  //todo: migrate to sqrl parser pos
  private final SqlParserPos pos;
  private final String message;

  public SqrlAstException(ErrorLabel errorLabel, SqlParserPos pos, String message, String... args) {
    super(message);
    this.errorLabel = errorLabel;
    this.pos = pos;
    this.message = String.format(message, (Object[]) args);
  }

  public ErrorLocation.FileLocation getLocation() {
    return toLocation(pos);
  }

  public static ErrorLocation.FileLocation toLocation(SqlParserPos pos) {
    return new FileLocation(pos.getLineNum(), pos.getColumnNum());
  }
  public static ErrorLocation.FileRange toRange(SqlParserPos pos) {
    return new FileRange(pos.getLineNum(), pos.getColumnNum(), pos.getEndLineNum(), pos.getEndColumnNum());
  }
}
