package ai.datasqrl.plan;

import ai.datasqrl.schema.LogicalDag;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Table;
import ai.datasqrl.plan.nodes.LogicalFlinkSink;
import ai.datasqrl.plan.nodes.LogicalPgSink;
import ai.datasqrl.plan.nodes.LogicalSqrlSink;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.commons.lang3.tuple.Pair;

public class SqrlPlanner {

  public void setDevQueries(LogicalDag dag) {
    final Set<Table> included = new HashSet<>();
    final Set<Table> toInclude = new HashSet<>();

    for (Table table : dag.getSchema().visibleList()) {
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
      assert queryTable.getRelNode()!=null;
      LogicalSqrlSink sink = new LogicalSqrlSink(queryTable.getRelNode().getCluster(), RelTraitSet.createEmpty(), queryTable.getRelNode(), queryTable);
      queryTable.setRelNode(sink);
    }
  }

  /**
   * Cuts the dag into two parts
   * - Flink pipeline nodes w/ postgres sinks
   * - Postgres table scans to materialized view sinks
   * @return
   */
  public Pair<List<LogicalFlinkSink>, List<LogicalPgSink>> optimize(LogicalDag dag) {
    cut(dag);

    //Convert sink to Physical
    final Set<Table> included = new HashSet<>();
    final Set<Table> toInclude = new HashSet<>();
    List<LogicalFlinkSink> flinkSinks = new ArrayList<>();

    dag.getSchema().visibleStream().filter(t -> t instanceof Table)
        .map(t -> (Table)t).forEach(t -> toInclude.add(t));

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
      if (queryTable.getRelNode() == null) continue;
      assert queryTable.getRelNode()!=null;
      if (queryTable.getRelNode() instanceof LogicalSqrlSink) {
        LogicalSqrlSink sink = (LogicalSqrlSink)queryTable.getRelNode();
        flinkSinks.add(new LogicalFlinkSink(sink.getCluster(), sink.getTraitSet(), sink.getInput(), sink.getQueryTable()));
      }
    }

    return Pair.of(flinkSinks, List.of());
  }

  private void cut(LogicalDag dag) {

  }
}
