package com.datasqrl.plan.global;

import static com.datasqrl.calcite.schema.ScriptPlanner.exportTable;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.plan.local.generate.ComputeTableFunction;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.datasqrl.plan.table.ProxyImportRelationalTable;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.plan.table.VirtualRelationalTable;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.tools.RelBuilder;

@Value
public class DAGPreparation {

  private final RelBuilder relBuilder;

  private final ErrorCollector errors;

  public Collection<AnalyzedAPIQuery> prepareInputs(SqrlSchema sqrlSchema, APIConnectorManager apiManager,
      Collection<ResolvedExport> exports, SqrlFramework framework) {
    //Add subscriptions as exports
    apiManager.getExports().forEach((sqrlTable, log) ->
        exports.add(exportTable(sqrlTable, log.getSink(), relBuilder, true)));

    //Assign timestamps to imports which propagate and restrict remaining timestamps in downstream tables
    StreamUtil.filterByClass(getAllPhysicalTables(sqrlSchema), ProxyImportRelationalTable.class)
        .forEach(this::finalizeTimestampOnTable);
    //Some downstream tables have multiple timestamp candidates because the timestamp was selected multiple times, pick the best one
    getAllPhysicalTables(sqrlSchema).forEach(this::finalizeTimestampOnTable);

    //Append timestamp column to nested, normalized tables, so we can propagate it
    //to engines that don't support denormalization
    sqrlSchema.getTableStream(VirtualRelationalTable.Child.class)
        .forEach(nestedTable -> {
          nestedTable.appendTimestampColumn(relBuilder.getTypeFactory());
        });

    //Replace default joins with inner joins for API queries
    return apiManager.getQueries().stream().map(apiQuery ->
      new AnalyzedAPIQuery(apiQuery.getNameId(), APIQueryRewriter.rewrite(relBuilder, apiQuery.getRelNode()))
    ).collect(Collectors.toList());
  }

  private Stream<PhysicalRelationalTable> getAllPhysicalTables(SqrlSchema sqrlSchema) {
    return Stream.concat(sqrlSchema.getTables(PhysicalRelationalTable.class).stream(),
            sqrlSchema.getFunctionStream(ComputeTableFunction.class).map(
                    ComputeTableFunction::getQueryTable));
  }

  private void finalizeTimestampOnTable(PhysicalRelationalTable table) {
    // Determine best timestamp if not already fixed
    if (table.getType().hasTimestamp() && !table.getTimestamp().hasFixedTimestamp()) {
      table.getTimestamp().selectTimestamp();
    }
    Preconditions.checkArgument(!table.getType().hasTimestamp() || table.getTimestamp().hasFixedTimestamp());
  }

  /**
   * Replaces default joins with inner joins
   */
  @AllArgsConstructor
  private static class APIQueryRewriter extends RelShuttleImpl {

    final RelBuilder relBuilder;

    public static RelNode rewrite(RelBuilder relBuilder, RelNode query) {
      APIQueryRewriter rewriter = new APIQueryRewriter(relBuilder);
      return query.accept(rewriter);
    }

    @Override
    public RelNode visit(LogicalJoin join) {
      if (join.getJoinType() == JoinRelType.DEFAULT) { //replace DEFAULT joins with INNER
        join = join.copy(join.getTraitSet(), join.getCondition(), join.getLeft(), join.getRight(),
            JoinRelType.INNER, join.isSemiJoinDone());
      }
      return super.visit(join);
    }
  }
}
