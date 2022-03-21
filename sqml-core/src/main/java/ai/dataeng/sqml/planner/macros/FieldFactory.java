package ai.dataeng.sqml.planner.macros;

import ai.dataeng.sqml.planner.AliasGenerator;
import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.TypeFactory;
import ai.dataeng.sqml.type.basic.BasicType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqrlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;

public class FieldFactory {
  static AliasGenerator aliasGenerator = new AliasGenerator();
  public static List<Field> createFields(SqlValidator validator, SqlNodeList selectList,
      SqlNodeList group) {
    List<Field> fields = new ArrayList<>();
    Set<String> groupOps = group == null ? new HashSet<>() : group.stream().map(e->e.toString()).collect(
        Collectors.toSet());

    for (SqlNode node : selectList.getList()) {
      RelDataType type = validator.getValidatedNodeType(node);
      if (type instanceof RelRecordType) continue; //Skip relationships
      BasicType basicType = TypeFactory.toBasicType(type);

      String ident = getColumnName(node);
      Column column = Column.createTemp(ident.split("\\$")[0], basicType, null);

      if (group != null) {
        String name = node.toString();
        if (node instanceof SqlBasicCall && ((SqlBasicCall) node).getOperator() == SqrlOperatorTable.AS) {
          name = ((SqlBasicCall) node).operand(0).toString();
        }
        if (groupOps.contains(name)) {
          column.setPrimaryKey(true);
        }
        if (name.startsWith("_")) {
          column.setForeignKey(true);
        }
      }
      if (group != null && group.getList().contains(node)) {
        column.setPrimaryKey(true);
      }
      fields.add(column);

    }

    return fields;
  }

  private static String getColumnName(SqlNode node) {
    if (node instanceof SqlIdentifier) {
      return ((SqlIdentifier)node).names.get(1);
    } else if(node instanceof SqlCall) {
      SqlCall call = (SqlCall)node;
      if (call.getOperator() == SqrlOperatorTable.AS) {
        return ((SqlIdentifier)(call.getOperandList().get(1))).names.get(0);
      }
    }

    return aliasGenerator.nextAnonymousName();
  }
}
