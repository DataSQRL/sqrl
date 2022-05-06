package ai.datasqrl.plan.util;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.Column;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

public class FlinkRelDataTypeConverter {

  public static List<Column> buildColumns(List<UnresolvedColumn> c) {
    List<UnresolvedPhysicalColumn> unresolvedColumns = c.stream()
        .map(column -> (UnresolvedPhysicalColumn) column)
        .collect(Collectors.toList());

    FlinkTypeFactory factory = new FlinkTypeFactory(new FlinkTypeSystem());

    List<Column> columns = new ArrayList<>();
    for (UnresolvedPhysicalColumn unresolvedColumn : unresolvedColumns) {
      //Drop nullability requirements
      RelDataTypeFieldImpl field = new RelDataTypeFieldImpl(unresolvedColumn.getName(), columns.size(),
          factory.createFieldTypeFromLogicalType(
              ((DataType) unresolvedColumn.getDataType()).getLogicalType()));
      boolean isInternal = !(field.getType() instanceof BasicSqlType);
      boolean isPrimaryKey = unresolvedColumn.getName().equalsIgnoreCase(Name.UUID.getCanonical());
      Column column = new Column(Name.system(unresolvedColumn.getName()),
          null, 0, 0, List.of(), isInternal, isPrimaryKey,
          field, new HashSet<>());
      columns.add(column);
    }

    return columns;
  }

  public static List<Integer> getScalarIndexes(Schema schema) {
    List<Integer> index = new ArrayList<>();
    List<UnresolvedColumn> columns = schema.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      UnresolvedColumn col = columns.get(i);
      UnresolvedPhysicalColumn column = (UnresolvedPhysicalColumn) col;
      if (column.getDataType() instanceof AtomicDataType) {
        index.add(i);
      }
    }

    return index;
  }
}
