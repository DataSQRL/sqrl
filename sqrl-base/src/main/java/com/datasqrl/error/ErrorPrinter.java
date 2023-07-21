/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.error;

import com.datasqrl.error.ErrorLocation.FileRange;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.stream.Collectors;

/**
 * Prints a pretty error message
 */
public class ErrorPrinter {

  public static final int LINES_BEFORE_LOCATION = 2;

  public static String prettyPrint(ErrorCollector errorCollector) {
    return prettyPrint(errorCollector.getErrors());
  }

  public static String prettyPrint(ErrorCollection errors) {
    return errors.stream()
        .map(ErrorPrinter::prettyPrint)
        .collect(Collectors.joining("\n"));
  }

  public static String getHead(ErrorMessage errorMessage) {
    return String.format("[%s] %s\n", errorMessage.getSeverity(), errorMessage.getMessage());
  }


  public static String prettyPrint(ErrorMessage errorMessage) {
    ErrorLocation location = errorMessage.getLocation();
    Preconditions.checkNotNull(location, "Error location can not be null");
    StringBuilder b = new StringBuilder();

    //print error severity and message
    b.append(getHead(errorMessage));
    //print error location
    String fileLocation = (location.hasPrefix()?String.format("%s:",location.getPrefix().toLowerCase()):"") +
        location.getPath();
    if (!Strings.isNullOrEmpty(fileLocation)) {
      b.append("in ").append(fileLocation);
      if (location.hasFile()) b.append(" [").append(location.getFile().toString()).append("]");
      b.append(":\n");
    }

    boolean addSeparator = false;
    if (location.hasFile()) {
      //print previous 2 lines
      //print line
      //print arrow pointing to offset
      FileRange fileRange = location.getFile();
      if (fileRange.isLocation()) {
        String codeSnippet = location.getSourceMap().getRange(new FileRange(Math.max(1,
            fileRange.getFromLine()-LINES_BEFORE_LOCATION),0,
            fileRange.getToLine(), Integer.MAX_VALUE));
        b.append(codeSnippet);
        b.append("-".repeat(fileRange.getFromOffset()));
        b.append("^\n");
      } else {
        //print arrow pointing down to offset
        //print range starting at fromOffset=0
        b.append("-".repeat(fileRange.getFromOffset()));
        b.append("v\n");
        String codeSnippet = location.getSourceMap().getRange(new FileRange(fileRange.getFromLine(),0,
            fileRange.getToLine(), fileRange.getToOffset()));
        b.append(codeSnippet).append("\n");
        addSeparator = true;
      }
    }
    //print error description (context)
    b.append(getErrorDescription(errorMessage, addSeparator));
    //print error code (if not generic)
//    if (label!=ErrorLabel.GENERIC) {
//      b.append("[").append(label.getLabel()).append("]");
//    }
    return b.toString();
  }

  public static String getErrorDescription(ErrorMessage errorMessage, boolean addSeparator) {
    ErrorLabel label = errorMessage.getErrorLabel();
    String result = label.getErrorDescription();
    if (!Strings.isNullOrEmpty(result)) {
      if (addSeparator) result = "--\n" + result;
      return result;
    } else {
      return "";
    }
  }
}
