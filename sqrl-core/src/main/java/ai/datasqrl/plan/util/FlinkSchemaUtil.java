package ai.datasqrl.plan.util;

import ai.datasqrl.parse.tree.name.Name;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.FieldsDataType;

public class FlinkSchemaUtil {

  public static List<Name> getFieldNames(Schema schema) {
    List<Name> fieldNames = schema.getColumns().stream()
        .map(e->Name.system(e.getName()))
        .collect(Collectors.toList());
    return fieldNames;
  }
}
