package ai.datasqrl.schema.constraints;

import ai.datasqrl.schema.Column;
import java.util.List;
import lombok.Value;
import org.apache.commons.lang3.tuple.Pair;

@Value
public class PrimaryKeyConstraint implements EqualityConstraint {
  List<Column> columns;
}
