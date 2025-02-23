package com.datasqrl.v2.parser;

import com.datasqrl.error.ErrorLocation.FileLocation;
import java.util.Optional;
import lombok.Value;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.sql.parser.error.SqlValidateException;
import org.apache.flink.table.api.ValidationException;

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
      String message = e.getMessage();
      message = message.replaceAll(" at line \\d*, column \\d*", ""); //remove line number from message
      return Optional.of(new MessageLocation(location, message));
    } else if (e instanceof CalciteContextException) {
      CalciteContextException calciteException = (CalciteContextException) e;
      FileLocation location = new FileLocation(calciteException.getPosLine(), calciteException.getPosColumn());
      String message = calciteException.getMessage();
      message = message.replaceAll("From line \\d*, column \\d* to line \\d*, column \\d*: ", ""); //remove line number from message
      return Optional.of(new MessageLocation(location, message));
    }
    return Optional.empty();
  }

  @Value
  public static class MessageLocation {
    FileLocation location;
    String message;
  }
}
