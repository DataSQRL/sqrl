package ai.datasqrl.plan;

import ai.datasqrl.config.scripts.SqrlQuery;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.nodes.LogicalFlinkSink;
import ai.datasqrl.plan.nodes.LogicalSqrlSink;
import ai.datasqrl.plan.queries.TableQuery;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Schema;
import ai.datasqrl.schema.Table;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.apache.calcite.plan.RelTraitSet;

@AllArgsConstructor
public class Optimizer {
//
//  private final Map<Name, SqrlQuery> queries;
//  private final boolean allowSchemaQueries;
//
//  public LogicalPlan findBestPlan(Schema schema) {
//    if (allowSchemaQueries) {
//      assignSchemaSinks(schema);
//    }
//
//    return optimize(schema);
//  }
//
//  /**
//   * Stub, just sets all queries to flink queries
//   */
//  private LogicalPlan optimize(Schema schema) {
//    cut(schema);
//
//    //Convert sink to Physical
//    final Set<Table> included = new HashSet<>();
//    final Set<Table> toInclude = new HashSet<>();
//    List<LogicalFlinkSink> flinkSinks = new ArrayList<>();
//
//    schema.visibleStream().filter(t -> t instanceof Table).forEach(t -> toInclude.add(t));
//
//    while (!toInclude.isEmpty()) {
//      Table next = toInclude.iterator().next();
//      assert !included.contains(next);
//      included.add(next);
//      toInclude.remove(next);
//      //Find all non-hidden related tables and add those
//      next.getFields().visibleStream().filter(f -> f instanceof Relationship && !f.getName().isHidden())
//          .map(f -> (Relationship) f)
//          .forEach(r -> {
////            Preconditions.checkArgument(!r.toTable.name.isHidden(),"Hidden tables should not be reachable by non-hidden relationships: " + r.toTable.name);
//            if (!included.contains(r.getToTable())) {
//              toInclude.add(r.getToTable());
//            }
//          });
//    }
//
//    List<TableQuery> queries = new ArrayList<>();
//    for (Table queryTable : included) {
//      if (queryTable.getHead() == null) {
//        continue;
//      }
//      assert queryTable.getHead() != null;
//      if (queryTable.getHead() instanceof LogicalSqrlSink) {
//        LogicalSqrlSink sink = (LogicalSqrlSink) queryTable.getHead();
//        flinkSinks.add(new LogicalFlinkSink(sink.getCluster(), sink.getTraitSet(), sink.getInput(0),
//            queryTable));
//        queries.add(new TableQuery(queryTable, sink));
//      }
//    }
//
//    return new LogicalPlan(queries, List.of(), schema);
//  }
//
//  private void cut(Schema schema) {
//
//  }
//
//
//  public static void assignSchemaSinks(Schema schema) {
//    final Set<Table> included = new HashSet<>();
//    final Set<Table> toInclude = new HashSet<>();
//
//    for (Table table : schema.visibleList()) {
//      toInclude.add(table);
//    }
//
//    while (!toInclude.isEmpty()) {
//      Table next = toInclude.iterator().next();
//      assert !included.contains(next);
//      included.add(next);
//      toInclude.remove(next);
//      //Find all non-hidden related tables and add those
//      next.getFields().visibleStream().filter(f -> f instanceof Relationship && !f.getName().isHidden())
//          .map(f -> (Relationship) f)
//          .forEach(r -> {
////            Preconditions.checkArgument(!r.toTable.name.isHidden(),"Hidden tables should not be reachable by non-hidden relationships: " + r.toTable.name);
//            if (!included.contains(r.getToTable())) {
//              toInclude.add(r.getToTable());
//            }
//          });
//    }
//
//    for (Table queryTable : included) {
//      if (queryTable.getHead() == null) {
//        continue;
//      }
//      LogicalSqrlSink sink = new LogicalSqrlSink(queryTable.getHead().getCluster(),
//          RelTraitSet.createEmpty(), queryTable.getHead(), queryTable);
////      queryTable.setRelNode(sink);
//    }
//  }
}
