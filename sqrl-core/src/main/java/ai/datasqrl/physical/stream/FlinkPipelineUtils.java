package ai.datasqrl.physical.stream;

import ai.datasqrl.schema.Table;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;

public class FlinkPipelineUtils {

  public static Schema addPrimaryKey(Schema toSchema, Table sqrlTable) {
    Schema.Builder builder = Schema.newBuilder();
    List<String> pks = new ArrayList<>();
    List<UnresolvedColumn> columns = toSchema.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      UnresolvedColumn column = columns.get(i);
      if (sqrlTable.getPrimaryKeys().contains(i)) {
        builder.column(column.getName(),
            ((UnresolvedPhysicalColumn) column).getDataType().notNull());
        pks.add(column.getName());
      } else {
        builder.column(column.getName(), ((UnresolvedPhysicalColumn) column).getDataType());
      }
    }

    return builder
//        .watermark(toSchema.getWatermarkSpecs().get(0).getColumnName(), toSchema.getWatermarkSpecs().get(0).getWatermarkExpression())
        .primaryKey(pks)
        .build();
  }
}
