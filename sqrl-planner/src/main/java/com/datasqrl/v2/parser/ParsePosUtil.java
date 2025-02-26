package com.datasqrl.v2.parser;

import com.datasqrl.error.ErrorLocation.FileLocation;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Value;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.sql.parser.error.SqlValidateException;
import org.apache.flink.table.api.ValidationException;

/**
 * Utility for mapping Flink/Calcite parser and planner errors to DataSQRL errors.
 * This means mapping line & column numbers as well as messages.
 */
public class ParsePosUtil {

  public static FileLocation convertPosition(SqlParserPos parsePos) {
    return new FileLocation(parsePos.getLineNum(), parsePos.getColumnNum());
  }

  public static Optional<MessageLocation> convertFlinkParserException(Exception e) {
    if (e instanceof StatementParserException) return Optional.empty();
    if (e.getCause() instanceof SqlParseException || e.getCause() instanceof SqlValidateException
      || e.getCause() instanceof CalciteContextException) {
      e = (Exception) e.getCause();
    }
    if (e instanceof SqlParseException || e instanceof SqlValidateException) {
      FileLocation location = ParsePosUtil.convertPosition((e instanceof SqlParseException)?((SqlParseException) e).getPos():
          ((SqlValidateException)e).getErrorPosition());
      String message = removeLineNumbersFromMessage(e.getMessage());
      return Optional.of(new MessageLocation(location, message));
    } else if (e instanceof CalciteContextException) {
      CalciteContextException calciteException = (CalciteContextException) e;
      FileLocation location = new FileLocation(calciteException.getPosLine(), calciteException.getPosColumn());
      String message = removeLineNumbersFromMessage(calciteException.getMessage());
      return Optional.of(new MessageLocation(location, message));
    }
    return Optional.empty();
  }

  private static final Pattern LINE_NUM_AT = Pattern.compile("at line \\d*, column \\d*[:\\s]?",
      Pattern.CASE_INSENSITIVE);

  private static final Pattern LINE_NUM_FROMTO = Pattern.compile("from line \\d*, column \\d* to line \\d*, column \\d*[:\\s]?",
      Pattern.CASE_INSENSITIVE);

  public static String removeLineNumbersFromMessage(String message) {
    Matcher matcher = LINE_NUM_AT.matcher(message);
    message = matcher.replaceAll("");
    matcher = LINE_NUM_FROMTO.matcher(message);
    message = matcher.replaceAll("");
    return message;
  }


  @Value
  public static class MessageLocation {
    FileLocation location;
    String message;
  }
}
