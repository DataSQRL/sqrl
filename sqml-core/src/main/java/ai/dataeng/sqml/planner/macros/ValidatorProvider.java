package ai.dataeng.sqml.planner.macros;

import ai.dataeng.sqml.planner.Planner;
import ai.dataeng.sqml.schema.Namespace;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.Optional;
import org.apache.calcite.sql.validate.SqlValidator;

public class ValidatorProvider {

  private final Planner planner;
  private final Optional<NamePath> tableName;
  private final Namespace namespace;

  public ValidatorProvider(Planner planner, Optional<NamePath> tableName, Namespace namespace) {

    this.planner = planner;
    this.tableName = tableName;
    this.namespace = namespace;
  }

  public SqlValidator create() {
    return planner.getValidator(tableName, namespace);
  }
}
