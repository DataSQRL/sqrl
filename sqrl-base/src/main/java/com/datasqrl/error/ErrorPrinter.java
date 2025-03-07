/*
 * Copyright © 2024 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.error;

import com.datasqrl.error.ErrorLocation.FileRange;
import java.util.stream.Collectors;

/** Prints a pretty error message */
public class ErrorPrinter {

  public static final int LINES_BEFORE_LOCATION = 2;

  public static String prettyPrint(ErrorCollector errorCollector) {
    return prettyPrint(errorCollector.getErrors());
  }

  public static String prettyPrint(ErrorCollection errors) {
    return errors.stream().map(ErrorPrinter::prettyPrint).collect(Collectors.joining("\n"));
  }

  public static String getHead(ErrorMessage errorMessage) {
    return String.format("[%s] %s\n", errorMessage.getSeverity(), errorMessage.getMessage());
  }

  public static String prettyPrint(ErrorMessage errorMessage) {
    ErrorLocation location = errorMessage.getLocation();
    //    Preconditions.checkNotNull(location, "Error location can not be null");
    StringBuilder b = new StringBuilder();

    // print error severity and message
    b.append(getHead(errorMessage));
    // print error location
    String fileLocation =
        (location.hasPrefix() ? String.format("%s:", location.getPrefix().toLowerCase()) : "")
            + location.getPath();
    if (!fileLocation.trim().isEmpty()) {
      b.append("in ").append(fileLocation);
      if (location.hasFile()) b.append(" [").append(location.getFile().toString()).append("]");
      b.append(":\n");
    }

    boolean addSeparator = false;
    if (location.hasFile() && !isAllZero(location.getFile())) {
      // print previous 2 lines
      // print line
      // print arrow pointing to offset
      FileRange fileRange = location.getFile();
      if (fileRange.isLocation()) {
        String codeSnippet =
            location
                .getSourceMap()
                .getRange(
                    new FileRange(
                        Math.max(1, fileRange.getFromLine() - LINES_BEFORE_LOCATION),
                        1,
                        fileRange.getToLine(),
                        Integer.MAX_VALUE));
        b.append(codeSnippet);
        b.append("-".repeat(Math.max(0, fileRange.getFromOffset() - 1)));
        b.append("^\n");
      } else {
        // print arrow pointing down to offset
        // print range starting at fromOffset=0
        b.append("-".repeat(fileRange.getFromOffset() - 1));
        b.append("v\n");
        String codeSnippet =
            location
                .getSourceMap()
                .getRange(
                    new FileRange(
                        fileRange.getFromLine(),
                        1,
                        fileRange.getToLine(),
                        fileRange.getToOffset()));
        b.append(codeSnippet).append("\n");
        addSeparator = true;
      }
    }
    // print error description (context)
    b.append(getErrorDescription(errorMessage, addSeparator));
    // print error code (if not generic)
    //    if (label!=ErrorLabel.GENERIC) {
    //      b.append("[").append(label.getLabel()).append("]");
    //    }
    return b.toString();
  }

  private static boolean isAllZero(FileRange file) {
    return file.getFromLine() == 0
        && file.getFromOffset() == 0
        && file.getToLine() == 0
        && file.getToOffset() == 0;
  }

  public static String getErrorDescription(ErrorMessage errorMessage, boolean addSeparator) {
    ErrorLabel label = errorMessage.getErrorLabel();
    String result = label.getErrorDescription();
    if (!(result == null || result.trim().isEmpty())) {
      if (addSeparator) result = "--\n" + result;
      return result;
    } else {
      return "";
    }
  }
}
