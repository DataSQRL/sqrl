package org.apache.calcite.sql;

import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class SqrlCompoundIdentifier extends SqlNodeList {

  private final List<SqlNode> items;

  public SqrlCompoundIdentifier(SqlParserPos pos, List<SqlNode> items) {
    super(List.of(), pos);
    this.items = items;
  }

  @Override
  public String toString() {
    //todo: show full table
    return super.toString();
  }

  public String getDisplay() {
    return items.stream()
        .map(i->{
          if (i instanceof SqlIdentifier) {
            return String.join(".", ((SqlIdentifier) i).names);
          } else if (i instanceof SqlCall) {
            return ((SqlCall) i).getOperator().getName();
          } else {
            return "{}";
          }
        }).collect(Collectors.joining("."));
  }
}
