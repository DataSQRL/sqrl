package ai.dataeng.sqml.catalog;

import ai.dataeng.sqml.planner.LogicalPlanImpl.Column;
import java.util.List;
import lombok.Value;

@Value
public class SqrlResolvedSchema {
  List<Column> columns;

  List<Column> uniqueKey;
  List<Column> primaryKey;
}
