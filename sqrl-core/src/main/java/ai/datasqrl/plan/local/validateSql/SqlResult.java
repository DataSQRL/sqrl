package ai.datasqrl.plan.local.validateSql;

import lombok.Value;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;

@Value
public class SqlResult {
  SqlNode sqlNode;
  SqlValidator sqlValidator;
}
