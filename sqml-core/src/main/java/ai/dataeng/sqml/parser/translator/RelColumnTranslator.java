package ai.dataeng.sqml.parser.translator;

import ai.dataeng.sqml.planner.Column;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;

public class RelColumnTranslator {

  public static List<Integer> getColumnIndices(List<Column> columns, RelDataType rowType) {
    Set<String> columnNames = columns.stream()
        .map(k->k.getName().getCanonical())
        .collect(Collectors.toSet());

    List<Integer> keyIndex = new ArrayList<>();
    for (int i = 0; i < rowType.getFieldList().size(); i++) {
      if (columnNames.contains(rowType.getFieldList().get(i).getName())) {
        keyIndex.add(i);
      }
    }

    Preconditions.checkState(columns.size() == keyIndex.size(), "Could not resolve all columns.");

    return keyIndex;
  }
}
