package ai.datasqrl.physical.stream.flink.plan;

import ai.datasqrl.plan.global.OptimizedDAG;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.types.AbstractDataType;

import java.util.ArrayList;
import java.util.List;

public class FlinkPipelineUtils {

  public static Schema addPrimaryKey(Schema toSchema, OptimizedDAG.TableSink persistTable) {
    Schema.Builder builder = Schema.newBuilder();
    List<String> pks = new ArrayList<>();
    List<UnresolvedColumn> columns = toSchema.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      UnresolvedColumn column = columns.get(i);
      AbstractDataType dataType = ((UnresolvedPhysicalColumn) column).getDataType();
      if (i < persistTable.getNumPrimaryKeys()) {
        dataType = dataType.notNull();
        pks.add(column.getName());
      }
      builder.column(column.getName(), dataType);
    }
    if (!pks.isEmpty()) builder.primaryKey(pks);
    return builder.build();
  }
}
