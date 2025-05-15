package com.datasqrl.graphql.util;

import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.planner.parser.StatementParserException;

import graphql.language.SourceLocation;

public class GraphqlCheckUtil {

  public static void checkState(boolean check, SourceLocation sourceLocation, String message, Object... args) {
    if (!check) {
      throw createThrowable(sourceLocation, message, args);
    }
  }

  public static StatementParserException createUnknownThrowable(String message, Object... args) {
    return createThrowable(new SourceLocation(0,0), message, args);
  }

  public static StatementParserException createThrowable(SourceLocation sourceLocation, String message, Object... args) {
    return new StatementParserException(ErrorLabel.GENERIC,
        toParserPos(sourceLocation),
        message, args);
  }

  public static FileLocation toParserPos(SourceLocation sourceLocation) {
    if (sourceLocation == null) {
      return new FileLocation(0,0);
    }
    return new FileLocation(sourceLocation.getLine(), sourceLocation.getColumn());
  }
}
