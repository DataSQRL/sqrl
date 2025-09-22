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

import com.datasqrl.error.ErrorLocation.FileLocation;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.sql.parser.error.SqlValidateException;

/**
 * Utility for mapping Flink/Calcite parser and planner errors to DataSQRL errors. This means
 * mapping line & column numbers as well as messages.
 */
public class ParsePosUtil {

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
    return Optional.empty();
  }

  private static final Pattern LINE_NUM_AT =
      Pattern.compile("at line \\d*, column \\d*[:\\s]?", Pattern.CASE_INSENSITIVE);

  private static final Pattern LINE_NUM_FROMTO =
      Pattern.compile(
          "from line \\d*, column \\d* to line \\d*, column \\d*[:\\s]?", Pattern.CASE_INSENSITIVE);

  public static String removeLineNumbersFromMessage(String message) {
    var matcher = LINE_NUM_AT.matcher(message);
    message = matcher.replaceAll("");
    matcher = LINE_NUM_FROMTO.matcher(message);
    message = matcher.replaceAll("");
    return message;
  }

  public record MessageLocation(FileLocation location, String message) {}
}
