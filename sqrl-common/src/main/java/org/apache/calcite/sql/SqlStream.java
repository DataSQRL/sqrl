package org.apache.calcite.sql;

import java.util.List;
import javax.annotation.Nonnull;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlStream extends SqlCall {

  public static final SqlStreamOperator OPERATOR = new SqlStreamOperator();
  private final StreamType type;
  private final SqlNode input;

  public SqlStream(SqlParserPos pos, StreamType type, SqlNode input) {
    super(pos);
    this.type = type;
    this.input = input;
  }

  @Nonnull
  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Nonnull
  @Override
  public List<SqlNode> getOperandList() {
    return List.of(input);
  }

  public SqlNode getQuery() {
    return input;
  }

  public static class SqlStreamOperator extends SqlOperator {

    private static final SqlWriter.FrameType FRAME_TYPE =
        SqlWriter.FrameTypeEnum.create("USING");

    //~ Constructors -----------------------------------------------------------

    private SqlStreamOperator() {
      super("STREAM", SqlKind.OTHER, 16, true, null, null, null);
    }

    //~ Methods ----------------------------------------------------------------

    public SqlSyntax getSyntax() {
      return SqlSyntax.SPECIAL;
    }

    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      //todo: fix stream type
      return new SqlStream(pos, StreamType.ADD, operands[0]);
    }

    @Override
    public void unparse(
        SqlWriter writer,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
      throw new RuntimeException("TODO unparse");
    }
  }
}
