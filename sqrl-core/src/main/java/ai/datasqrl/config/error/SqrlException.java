package ai.datasqrl.config.error;

import ai.datasqrl.plan.calcite.util.SqlNodePrinter;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class SqrlException extends RuntimeException {

  private final ErrorCode errorCode;
  private final Optional<SqlNode> currentNode;
  private final String context;

  public SqrlException(ErrorCode code, Optional<SqlNode> currentNode, SqlParserPos pos, String context) {
    super(formatMessage(code, currentNode, pos, context));
    this.errorCode = code;
    this.currentNode = currentNode;
    this.context = context;
  }

  public SqrlException(ErrorCode code, SqlNode currentNode, SqlParserPos pos, String context) {
    this(code, Optional.of(currentNode), pos, context);
  }

  private static String formatMessage(ErrorCode code, Optional<SqlNode> currentNode, SqlParserPos pos, String message) {
    return "Error: "
        + "\n"
        + currentNode.map(SqlNodePrinter::toString).orElse("")
        + "\n"
        + message
        + "\n\n"
        + code.getError();
  }

}
