package ai.datasqrl.plan;

import ai.datasqrl.plan.nodes.LogicalSqrlSink;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Schema;
import ai.datasqrl.schema.Table;
import java.util.HashSet;
import java.util.Set;
import org.apache.calcite.plan.RelTraitSet;

public class SqrlPlanner {

  public static void assignSchemaSinks(Schema schema) {
    final Set<Table> included = new HashSet<>();
    final Set<Table> toInclude = new HashSet<>();

    for (Table table : schema.visibleList()) {
      toInclude.add(table);
    }

    while (!toInclude.isEmpty()) {
      Table next = toInclude.iterator().next();
      assert !included.contains(next);
      included.add(next);
      toInclude.remove(next);
      //Find all non-hidden related tables and add those
      next.getFields().visibleStream().filter(f -> f instanceof Relationship && !f.name.isHidden())
          .map(f -> (Relationship)f)
          .forEach(r -> {
//            Preconditions.checkArgument(!r.toTable.name.isHidden(),"Hidden tables should not be reachable by non-hidden relationships: " + r.toTable.name);
            if (!included.contains(r.toTable)) {
              toInclude.add(r.toTable);
            }
          });
    }

    for (Table queryTable : included) {
      if (queryTable.getRelNode() == null )continue;
      LogicalSqrlSink sink = new LogicalSqrlSink(queryTable.getRelNode().getCluster(), RelTraitSet.createEmpty(), queryTable.getRelNode(), queryTable);
      queryTable.setRelNode(sink);
    }
  }
}
