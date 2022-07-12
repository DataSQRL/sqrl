package ai.datasqrl.plan.local.generate.node.util;

import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

public class SqlNodeUtil {

  public static List<SqlNode> toSelectList(Optional<String> alias, List<RelDataTypeField> fieldList) {
    return fieldList.stream()
        .map(field -> fieldToNode(alias, field))
        .collect(Collectors.toList());
  }

  public static SqlNode fieldToNode(Optional<String> alias, RelDataTypeField field) {
    List<String> name = new ArrayList<>();
    alias.ifPresent(a->name.add(a));
    name.add(field.getName());

    return new SqlIdentifier(name, SqlParserPos.ZERO);
  }

  public static SqlNode and(List<SqlNode> expressions) {
    if (expressions.size() == 0) {
      return null;
    } else if (expressions.size() == 1) {
      return expressions.get(0);
    } else if (expressions.size() == 2) {
      return new SqlBasicCall(SqrlOperatorTable.AND,
          new SqlNode[]{
              expressions.get(0),
              expressions.get(1)
          },
          SqlParserPos.ZERO);
    }

    return new SqlBasicCall(SqrlOperatorTable.AND,
        new SqlNode[]{
            expressions.get(0),
            and(expressions.subList(1, expressions.size()))
        },
        SqlParserPos.ZERO);
  }

  public static String printJoin(SqlNode from) {

    SqlPrettyWriter sqlWriter = new SqlPrettyWriter();
    sqlWriter.startList("", "");
    from.unparse(sqlWriter, 0, 0);
    return sqlWriter.toString();
  }
}
