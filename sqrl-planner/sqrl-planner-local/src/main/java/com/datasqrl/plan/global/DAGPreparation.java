package com.datasqrl.plan.global;

import static com.datasqrl.calcite.schema.ScriptPlanner.exportTable;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.plan.local.generate.ComputeTableFunction;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.datasqrl.plan.table.ProxyImportRelationalTable;
import com.datasqrl.plan.table.ScriptRelationalTable;
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
        exports.add(exportTable(sqrlTable, log.getSink(), relBuilder)));

    List<ScriptRelationalTable> scriptTables = sqrlSchema.getTables(ScriptRelationalTable.class);
    //Assign timestamps to imports which should propagate and set all remaining timestamps
    //This also replaces the RelNode for import tables with the actual import table in preparation
    //for stitching the DAG together.
    StreamUtil.filterByClass(scriptTables, ProxyImportRelationalTable.class)
        .forEach(this::finalizeSourceTable);
    //Check that all script tables and the query tables of ComputeTableFunctions have timestamps
    Preconditions.checkArgument(Stream.concat(scriptTables.stream(),
        sqrlSchema.getFunctionStream(ComputeTableFunction.class).map(
            ComputeTableFunction::getQueryTable))
        .allMatch(table -> !table.getType().hasTimestamp() || table.getTimestamp().hasFixedTimestamp()));

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

  private void finalizeSourceTable(ProxyImportRelationalTable table) {
    // Determine timestamp
    if (!table.getTimestamp().hasFixedTimestamp()) {
      table.getTimestamp().getBestCandidate().lockTimestamp();
    }
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
