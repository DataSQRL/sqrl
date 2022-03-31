package ai.dataeng.sqml.parser.sqrl.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.types.DataType;

public class StreamTable extends AbstractTable {

  RelDataTypeImpl relDataType;
  public StreamTable(Schema schema) {
    List<UnresolvedPhysicalColumn> columns = schema.getColumns().stream()
        .map(column->(UnresolvedPhysicalColumn) column)
        .collect(Collectors.toList());

    FlinkTypeFactory factory = new FlinkTypeFactory(new FlinkTypeSystem());

    List<RelDataTypeField> fields = new ArrayList<>();
    for (UnresolvedPhysicalColumn column : columns) {
      fields.add(new RelDataTypeFieldImpl(column.getName(), fields.size(), factory.createFieldTypeFromLogicalType(((DataType)column.getDataType()).getLogicalType())));
    }

    this.relDataType = new StreamDataType(fields);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return relDataType;
  }


  public static class StreamDataType extends RelDataTypeImpl {
    public StreamDataType(
        List<? extends RelDataTypeField> fieldList) {
      super(fieldList);
    }

    @Override
    protected void generateTypeString(StringBuilder stringBuilder, boolean b) {

    }
  }
}
