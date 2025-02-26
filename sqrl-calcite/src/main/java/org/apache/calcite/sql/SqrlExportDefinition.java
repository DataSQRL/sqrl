package org.apache.calcite.sql;

import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class SqrlExportDefinition extends SqrlStatement {

  protected final SqlIdentifier tablePath;
  protected final SqlIdentifier sinkPath;

  public SqrlExportDefinition(
      SqlParserPos location, SqlIdentifier tablePath, SqlIdentifier sinkPath) {
    super(location, tablePath, Optional.empty());
    this.tablePath = tablePath;
    this.sinkPath = sinkPath;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}
