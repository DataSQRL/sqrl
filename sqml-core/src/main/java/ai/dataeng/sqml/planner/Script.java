package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.catalog.Namespace;
import lombok.ToString;
import lombok.Value;

@Value
@ToString
public class Script {
  Namespace namespace;
  LogicalPlan plan;
}
