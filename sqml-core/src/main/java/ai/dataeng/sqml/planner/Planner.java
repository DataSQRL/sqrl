package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.Optional;

public interface Planner {

  PlannerResult plan(Optional<NamePath> context, Namespace namespace, String sql);
}
