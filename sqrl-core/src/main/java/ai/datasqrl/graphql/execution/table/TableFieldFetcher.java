package ai.datasqrl.graphql.execution.table;

import ai.datasqrl.graphql.execution.criteria.Criteria;
import java.util.Optional;
import lombok.Value;

@Value
public class TableFieldFetcher {
  H2Table table;
  Optional<Criteria> criteria;
}
