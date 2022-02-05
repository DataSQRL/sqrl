package ai.dataeng.execution.table;

import ai.dataeng.execution.criteria.Criteria;
import java.util.Optional;
import lombok.Value;

@Value
public class TableFieldFetcher {
  H2Table table;
  Optional<Criteria> criteria;
}
