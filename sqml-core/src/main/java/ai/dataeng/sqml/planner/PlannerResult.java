package ai.dataeng.sqml.planner;

import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;

@Value
public class PlannerResult {
  RelNode root;
  SqlNode node;
  SqlValidator validator;
}
