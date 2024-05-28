package com.datasqrl.calcite.dialect.snowflake;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlForeignKeyConstraint extends SqlConstraint {

  private final SqlIdentifier referenceTableName;
  private final SqlNodeList referenceColumnNames;

  public SqlForeignKeyConstraint(SqlParserPos pos, SqlIdentifier constraintName,
      SqlIdentifier referenceTableName, SqlNodeList referenceColumnNames) {
    super(pos, constraintName);
    this.referenceTableName = Objects.requireNonNull(referenceTableName);
    this.referenceColumnNames = Objects.requireNonNull(referenceColumnNames);
  }

  @Nonnull
  @Override
  public List<SqlNode> getOperandList() {
    return List.of(constraintName, referenceTableName, referenceColumnNames);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (constraintName != null) {
      writer.keyword("CONSTRAINT");
      constraintName.unparse(writer, leftPrec, rightPrec);
    }
    writer.keyword("FOREIGN KEY");
    writer.keyword("REFERENCES");
    referenceTableName.unparse(writer, leftPrec, rightPrec);
    if (referenceColumnNames != null && !referenceColumnNames.getList().isEmpty()) {
      SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
      referenceColumnNames.unparse(writer, 0, 0);
      writer.endList(frame);
    }
  }
}
