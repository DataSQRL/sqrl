package org.apache.calcite.sql;

import ai.datasqrl.parse.tree.name.NamePath;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class DistinctAssignment extends Assignment {

  private final SqlNode table;
  private final List<SqlNode> partitionKeys;
  private final List<SqlNode> order;
  private final SqlNode query;

  public DistinctAssignment(SqlParserPos location, NamePath name, SqlNode table,
     List<SqlNode> partitionKeys,
      List<SqlNode> order, Optional<SqlNodeList> hints, SqlNode query) {
    super(location, name, hints);
    this.table = table;
    this.partitionKeys = partitionKeys;
    this.order = order;
    this.query = query;
  }

  @Override
  public void unparse(SqlWriter sqlWriter, int i, int i1) {
    super.unparse(sqlWriter, i, i1);
    sqlWriter.keyword("DISTINCT");
    table.unparse(sqlWriter, i, i1);
    sqlWriter.print("ON ...");
  }
}
