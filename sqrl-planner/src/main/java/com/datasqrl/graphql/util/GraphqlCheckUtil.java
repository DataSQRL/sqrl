package com.datasqrl.graphql.util;

import com.datasqrl.error.ErrorLabel;
import com.datasqrl.parse.SqrlAstException;
import graphql.language.SourceLocation;
import org.apache.calcite.sql.parser.SqlParserPos;

public class GraphqlCheckUtil {

  public static void checkState(
      boolean check, SourceLocation sourceLocation, String message, Object... args) {
    if (!check) {
      throw createThrowable(sourceLocation, message, args);
    }
  }

  public static SqrlAstException createUnknownThrowable(String message, Object... args) {
    return createThrowable(new SourceLocation(0, 0), message, args);
  }

  public static SqrlAstException createThrowable(
      SourceLocation sourceLocation, String message, Object... args) {
    return new SqrlAstException(ErrorLabel.GENERIC, toParserPos(sourceLocation), message, args);
  }

  public static SqlParserPos toParserPos(SourceLocation sourceLocation) {
    if (sourceLocation == null) {
      return new SqlParserPos(0, 0);
    }
    return new SqlParserPos(sourceLocation.getLine(), sourceLocation.getColumn());
  }
}
