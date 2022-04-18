package ai.datasqrl.plan;

import ai.datasqrl.config.scripts.SqrlQuery;
import ai.datasqrl.schema.Table;
import java.util.Optional;
import lombok.Value;
import org.apache.calcite.rel.RelNode;

@Value
public class RelQuery {
  Optional<SqrlQuery> query;
  Table table;
  RelNode relNode;
}
