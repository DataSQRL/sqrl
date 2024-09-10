/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.parse;

import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorLocation;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.error.ErrorLocation.FileRange;
import java.util.Optional;
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

  public SqrlAstException(ErrorLabel errorLabel, SqlParserPos pos, String message, Object... args) {
    this(Optional.empty(), errorLabel, pos, message, args);
  }

  public SqrlAstException(Optional<Throwable> cause, ErrorLabel errorLabel, SqlParserPos pos, String message, Object... args) {
    super(message, cause.orElse(null), true, true);
    this.errorLabel = errorLabel;
    this.pos = pos;
    this.message = message == null ? null :String.format(message, args);
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
