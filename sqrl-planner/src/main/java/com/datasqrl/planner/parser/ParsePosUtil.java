/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
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
package com.datasqrl.planner.parser;

import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLocation.FileLocation;
import java.util.Objects;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.sql.parser.error.SqlValidateException;
import org.apache.flink.table.api.ValidationException;

/**
 * Utility for mapping Flink/Calcite parser and planner errors to DataSQRL errors. This means
 * mapping line & column numbers as well as messages.
 */
public class ParsePosUtil {

  /**
   * Matches a position at the end of a sentence in a Flink message, e.g. "..., at line 4, column
   * 17\n", including the separators around it.
   */
  private static final Pattern POSITION_IN_MESSAGE =
      Pattern.compile(
          "[,\\s]*\\bat line (?<line>\\d+), column (?<column>\\d+)(?<period>\\.?)\\s*",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern LINE_NUM_AT =
      Pattern.compile("at line \\d*, column \\d*[:\\s]?", Pattern.CASE_INSENSITIVE);

  private static final Pattern LINE_NUM_FROMTO =
      Pattern.compile(
          "from line \\d*, column \\d* to line \\d*, column \\d*[:\\s]?", Pattern.CASE_INSENSITIVE);

  public static FileLocation convertPosition(SqlParserPos parsePos) {
    return new FileLocation(parsePos.getLineNum(), parsePos.getColumnNum());
  }

  public static Optional<MessageLocation> convertFlinkParserException(Throwable e) {
    if (e instanceof StatementParserException) {
      return Optional.empty();
    }
    if (e.getCause() instanceof SqlParseException
        || e.getCause() instanceof SqlValidateException
        || e.getCause() instanceof CalciteContextException) {
      e = e.getCause();
    }
    if (e instanceof SqlParseException || e instanceof SqlValidateException) {
      var location =
          ParsePosUtil.convertPosition(
              (e instanceof SqlParseException spe)
                  ? spe.getPos()
                  : ((SqlValidateException) e).getErrorPosition());
      var message = removeLineNumbersFromMessage(e.getMessage());
      return Optional.of(new MessageLocation(location, message));
    } else if (e instanceof CalciteContextException calciteException) {
      var location =
          new FileLocation(calciteException.getPosLine(), calciteException.getPosColumn());
      var message = removeLineNumbersFromMessage(calciteException.getMessage());
      return Optional.of(new MessageLocation(location, message));
    }
    // Some Flink validation errors (e.g. for CREATE TABLE) only have the position in the message
    return ExceptionUtils.getThrowableList(e).stream()
        .filter(ValidationException.class::isInstance)
        .map(Throwable::getMessage)
        .filter(Objects::nonNull)
        .flatMap(message -> extractPositionFromMessage(message).stream())
        .findFirst();
  }

  /**
   * Reports an error at the location of the collector, e.g. a statement or a table that is defined
   * in another file.
   *
   * @param mapSqlLocation maps the position of a Flink parser or validator error in the SQL to its
   *     position relative to the location of the collector
   * @return the reported error
   */
  public static RuntimeException reportError(
      ErrorCollector errors, Throwable e, UnaryOperator<FileLocation> mapSqlLocation) {
    if (e instanceof VirtualMachineError vmError) {
      // E.g. out of memory, so stop planning instead of planning the remaining statements
      throw vmError;
    }
    // Already reported, and it wraps the original error, which would be reported again below
    if (e instanceof CollectedException collected) {
      return collected;
    }

    var converted = convertFlinkParserException(e);
    if (converted.isPresent()) {
      var location = errors.getLocation();
      var base = location.hasFile() ? location.getFileLocation() : FileLocation.START;
      return errors
          .atFile(base.add(mapSqlLocation.apply(converted.get().location())))
          .exception("%s", converted.get().message());
    }
    if (e instanceof ValidationException && e.getCause() != null) {
      e = e.getCause();
    }
    // Print stack trace for unknown exceptions
    if (e.getMessage() == null
        || e instanceof IllegalStateException
        || e instanceof NullPointerException) {
      e.printStackTrace();
    }
    // Use registered error handlers
    return errors.handle(e);
  }

  /**
   * Extracts the position from a Flink message and removes it from the message. If the message
   * continues after the position, the rest follows as the next sentence on the same line.
   */
  private static Optional<MessageLocation> extractPositionFromMessage(String message) {
    var matcher = POSITION_IN_MESSAGE.matcher(message);
    if (!matcher.find()) {
      return Optional.empty();
    }

    var line = Integer.parseInt(matcher.group("line"));
    if (line < 1) { // Flink uses line 0 when the position is unknown
      return Optional.empty();
    }

    var location = new FileLocation(line, Integer.parseInt(matcher.group("column")));
    var endsMessage = matcher.end() == message.length();
    var separator = endsMessage ? matcher.group("period") : ". ";
    var cleaned =
        message.substring(0, matcher.start()) + separator + message.substring(matcher.end());

    return Optional.of(new MessageLocation(location, cleaned));
  }

  public static String removeLineNumbersFromMessage(String message) {
    var matcher = LINE_NUM_AT.matcher(message);
    message = matcher.replaceAll("");
    matcher = LINE_NUM_FROMTO.matcher(message);
    message = matcher.replaceAll("");
    return message;
  }

  public record MessageLocation(FileLocation location, String message) {}
}
