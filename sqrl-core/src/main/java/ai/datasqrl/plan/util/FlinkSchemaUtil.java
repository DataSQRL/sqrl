package ai.datasqrl.plan.util;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.VersionedName;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.types.AtomicDataType;

public class FlinkSchemaUtil {

  public static List<Name> getFieldNames(RelNode relNode) {
    List<Name> fieldNames = relNode.getRowType().getFieldNames().stream()
        .map(e -> VersionedName.parse(e).toName())
        .collect(Collectors.toList());
    return fieldNames;
  }

  public static int getIndex(Schema schema, String name) {
    for (int i = 0; i < schema.getColumns().size(); i++) {
      if (schema.getColumns().get(i).getName().equalsIgnoreCase(name)) {
        return i;
      }
    }

    return -1;
  }

  public static int getIndex(RelDataType type, String name) {
    for (int i = 0; i < type.getFieldList().size(); i++) {
      if (type.getFieldList().get(i).getName().equalsIgnoreCase(name)) {
        return i;
      }
    }

    return -1;
  }

  public static boolean requiresShredding(Schema schema) {
    for (UnresolvedColumn col : schema.getColumns()) {
      UnresolvedPhysicalColumn column = (UnresolvedPhysicalColumn) col;
      if (!(column.getDataType() instanceof AtomicDataType)) {
        return true;
      }
    }
    return false;
  }
}
