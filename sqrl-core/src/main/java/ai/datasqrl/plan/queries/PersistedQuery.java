package ai.datasqrl.plan.queries;

import ai.datasqrl.config.scripts.SqrlQuery;
import ai.datasqrl.schema.Table;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;

/**
 * A persisted query for additional optimization
 */
public class PersistedQuery extends TableQuery {
  Optional<SqrlQuery> query;
  public PersistedQuery(Table table, RelNode relNode, Optional<SqrlQuery> query) {
    super(table, relNode);
    this.query = query;
  }
}
