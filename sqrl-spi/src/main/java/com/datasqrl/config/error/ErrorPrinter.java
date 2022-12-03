package com.datasqrl.config.error;

import java.util.stream.Collectors;

/**
 * Prints a pretty error message
 */
public class ErrorPrinter {

  public static String prettyPrint(ErrorCollector errorCollector) {
    return errorCollector.getAll().stream()
        .map(ErrorPrinter::prettyPrint)
        .collect(Collectors.joining("\n"));
  }

  public static String prettyPrint(ErrorMessage errorMessage) {
    ErrorLocation location = errorMessage.getLocation();
    SourceMap sourceMap = errorMessage.getSourceMap();

    StringBuilder b = new StringBuilder();
    String source = sourceMap.getSource();

    String[] src = source.split("\n");

    //print previous 2 lines
    //print line
    //print arrow
    //print error message (context)
    //print error code
    b.append(String.format("[%s]\n", errorMessage.getSeverity()));

    if (location.getFile() != null && location.getFile().getOffset() > -1) {
      int linesBefore = 2;
      for (int i = Math.max(0, location.getFile().getLine() - linesBefore);
          i < location.getFile().getLine() && i < src.length; i++) {
        b.append(src[i])
            .append("\n");
      }

      b.append("-".repeat(location.getFile().getOffset()));
      b.append("^  ");
    }
    b.append(errorMessage.getMessage())
        .append("\n");

    errorMessage.getErrorCode().map(c->b.append(c.getError()));
    return b.toString();
  }
}
