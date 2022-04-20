package ai.datasqrl.plan.util;

import ai.datasqrl.plan.nodes.StreamTable.StreamDataType;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;

public class FlinkRelDataTypeConverter {

  public static RelDataType toRelDataType(List<UnresolvedColumn> c) {
    List<UnresolvedPhysicalColumn> columns = c.stream()
        .map(column->(UnresolvedPhysicalColumn) column)
        .collect(Collectors.toList());

    FlinkTypeFactory factory = new FlinkTypeFactory(new FlinkTypeSystem());

    List<RelDataTypeField> fields = new ArrayList<>();
    for (UnresolvedPhysicalColumn column : columns) {
      //Drop nullability requirements
      RelDataTypeFieldImpl field = new RelDataTypeFieldImpl(column.getName(), fields.size(),
          factory.createFieldTypeFromLogicalType(((DataType)column.getDataType()).getLogicalType()));

      fields.add(field);
    }

    return new StreamDataType(null, fields);
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
