package ai.dataeng.sqml.analyzer2;

import ai.dataeng.sqml.logical4.LogicalPlan;
import ai.dataeng.sqml.logical4.LogicalPlan.Column;
import java.util.List;
import lombok.Value;

@Value
public class SqrlResolvedSchema {
  List<Column> columns;

  List<Column> uniqueKey;
  List<Column> primaryKey;
}
