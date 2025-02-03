package com.datasqrl.v2.parser;

import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorLocation.FileLocation;

public class StatementParserException extends RuntimeException {

  ErrorLabel errorLabel;
  FileLocation fileLocation;

  public StatementParserException(FileLocation fileLocation, Exception e) {
    this(fileLocation, e, e.getMessage());
  }

  public StatementParserException(FileLocation fileLocation, Exception e, String message) {
    super(message, e);
    this.fileLocation = fileLocation;
    this.errorLabel = ErrorLabel.GENERIC;
  }

  public StatementParserException(ErrorLabel errorLabel, FileLocation fileLocation, String message, Object... args) {
    super(args.length==0?message:String.format(message,args));
    this.fileLocation  = fileLocation;
    this.errorLabel = errorLabel;
  }

  public static void checkFatal(boolean expression, ErrorLabel errorLabel, String message, Object... args) {
    checkFatal(expression, FileLocation.START, errorLabel, message, args);
  }

  public static void checkFatal(boolean expression, FileLocation fileLocation, ErrorLabel errorLabel, String message, Object... args) {
    if (!expression) {
      throw new StatementParserException(errorLabel, fileLocation, message, args);
    }
  }

//  public static StatementParserException from(Exception e, FileLocation reference, int firstRowAddition) {
//    if (e.getCause() instanceof SqlParseException) {
//      SqlParseException cause = (SqlParseException) e.getCause();
//      FileLocation location = convertPosition(cause.getPos());
//      if (location.getLine()==1) {
//        location = new FileLocation(1, Math.max(location.getOffset()-firstRowAddition,1));
//      }
//      location = reference.add(location);
//      String message = cause.getMessage();
//      message = message.replaceAll(" at line \\d*, column \\d*", ""); //remove line number from message
//      return new StatementParserException(location, e, message);
//    } else {
//      return new StatementParserException(reference, e);
//    }
//  }




}
