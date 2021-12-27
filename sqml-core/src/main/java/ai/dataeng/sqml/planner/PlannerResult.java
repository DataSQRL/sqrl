package ai.dataeng.sqml.planner;

import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;

@Value
public class PlannerResult {
  RelNode root;
  SqlNode node;
}
