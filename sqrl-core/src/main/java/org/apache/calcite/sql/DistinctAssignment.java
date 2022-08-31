package org.apache.calcite.sql;

import ai.datasqrl.parse.tree.name.NamePath;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class DistinctAssignment extends Assignment {

  private final SqlNode table;
  private final List<SqlNode> partitionKeys;
  private final List<SqlNode> order;
  private final SqlNodeList hints;
  private final SqlNode query;

  public DistinctAssignment(SqlParserPos location, NamePath name, SqlNode table,
     List<SqlNode> partitionKeys,
      List<SqlNode> order, SqlNodeList hints, SqlNode query) {
    super(location, name);
    this.table = table;
    this.partitionKeys = partitionKeys;
    this.order = order;
    this.hints = hints;
    this.query = query;
  }
}
