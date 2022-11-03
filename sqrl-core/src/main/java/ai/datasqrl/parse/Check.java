package ai.datasqrl.parse;

import static ai.datasqrl.config.error.ErrorCode.GENERIC_ERROR;

import ai.datasqrl.config.error.ErrorCode;
import ai.datasqrl.config.error.SqrlException;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.Resources.ExInst;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorException;

public class Check {

  public static RuntimeException newException(ErrorCode code, SqlNode currentNode,
      SqlParserPos pos, String message) {
    return new SqrlException(code, currentNode, pos, message);
  }

  public static <T> void state(boolean check, ErrorCode code, SqlNode currentNode, String message) {
    state(check, code, currentNode, () -> currentNode.getParserPosition(), () -> message);
  }

  public static void state(boolean check, ErrorCode code, SqlNode currentNode, Supplier<String> message) {
    if (!check) {
      state(check, code, currentNode, ()->currentNode.getParserPosition(), message);
    }
  }

  public static void state(boolean check, ErrorCode code, SqlNode currentNode,
      Supplier<SqlParserPos> pos, Supplier<String> message) {
    if (!check) {
      throw new SqrlException(code, currentNode, pos.get(), message.get());
    }
  }

  public static CalciteContextException newContextException(SqlParserPos pos,
      ExInst<SqlValidatorException> e) {
    throw new SqrlException(GENERIC_ERROR,
        Optional.empty(), pos, e.ex().getMessage());
  }
}
