package ai.datasqrl.config.error;

import org.apache.calcite.util.SqlNodePrinter;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class SqrlAstException extends RuntimeException {

  private final ErrorCode errorCode;
  private final SqlParserPos pos;
  private final String message;

  public SqrlAstException(ErrorCode errorCode, SqlParserPos pos, String message) {
    super(message);
    this.errorCode = errorCode;
    this.pos = pos;
    this.message = message;
  }

}
