package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.schema.Namespace;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.Optional;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;

public interface Planner {
  SqlNode parse(String sql);
  SqlValidator getValidator(Optional<NamePath> context, Namespace namespace);
}
