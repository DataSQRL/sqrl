package com.datasqrl.plan.global;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.calcite.table.ProxyImportRelationalTable;
import com.datasqrl.plan.calcite.table.ScriptRelationalTable;
import com.datasqrl.plan.calcite.table.VirtualRelationalTable;
import com.datasqrl.plan.calcite.util.CalciteUtil;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.tools.RelBuilder;

@Value
public class DAGPreparation {

  private final RelBuilder relBuilder;

  private final ErrorCollector errors;

  public Collection<AnalyzedAPIQuery> prepareInputs(CalciteSchema relSchema, Collection<APIQuery> queries) {
    List<ScriptRelationalTable> scriptTables = CalciteUtil.getTables(relSchema,
        ScriptRelationalTable.class);
    //Assign timestamps to imports which should propagate and set all remaining timestamps
    //This also replaces the RelNode for import tables with the actual import table in preparation
    //for stitching the DAG together.
    StreamUtil.filterByClass(scriptTables, ProxyImportRelationalTable.class)
        .forEach(this::finalizeSourceTable);
    Preconditions.checkArgument(scriptTables.stream().allMatch(
        table -> !table.getType().hasTimestamp() || table.getTimestamp().hasFixedTimestamp()));

    //Append timestamp column to nested, normalized tables, so we can propagate it
    //to engines that don't support denormalization
    StreamUtil.filterByClass(CalciteUtil.getTables(relSchema,
            VirtualRelationalTable.class).stream(),VirtualRelationalTable.Child.class)
        .forEach(nestedTable -> {
          nestedTable.appendTimestampColumn(relBuilder.getTypeFactory());
        });

    //Replace default joins with inner joins for API queries
    return queries.stream().map(apiQuery -> {
      return new AnalyzedAPIQuery(apiQuery.getNameId(), APIQueryRewriter.rewrite(relBuilder, apiQuery.getRelNode()));
    }).collect(Collectors.toList());
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
