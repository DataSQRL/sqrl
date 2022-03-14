package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.Optional;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.SqrlRelBuilder;
import org.apache.calcite.rel.RelNode;

public interface Planner {
  PlannerResult plan(Optional<NamePath> context, Namespace namespace, String sql);
  SqrlRelBuilder getRelBuilder(Optional<NamePath> context, Namespace namespace);
  SqlValidator getValidator(Namespace namespace);
}
