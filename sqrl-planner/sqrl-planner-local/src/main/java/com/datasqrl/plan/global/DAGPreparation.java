package com.datasqrl.plan.global;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.local.generate.QueryTableFunction;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.datasqrl.plan.table.ProxyImportRelationalTable;
import com.datasqrl.plan.table.ScriptRelationalTable;
import com.datasqrl.plan.table.VirtualRelationalTable;
import com.datasqrl.schema.SQRLTable;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

@Value
public class DAGPreparation {

  private final RelBuilder relBuilder;

  private final ErrorCollector errors;

  public Collection<AnalyzedAPIQuery> prepareInputs(SqrlSchema sqrlSchema, APIConnectorManager apiManager,
      Collection<ResolvedExport> exports, SqrlFramework framework) {
    //Add subscriptions as exports
    apiManager.getExports().forEach((sqrlTable, log) ->
        exports.add(exportTable(sqrlTable, log.getSink(), relBuilder, framework)));

    List<ScriptRelationalTable> scriptTables = sqrlSchema.getTables(ScriptRelationalTable.class);
    //Assign timestamps to imports which should propagate and set all remaining timestamps
    //This also replaces the RelNode for import tables with the actual import table in preparation
    //for stitching the DAG together.
    StreamUtil.filterByClass(scriptTables, ProxyImportRelationalTable.class)
        .forEach(this::finalizeSourceTable);
    //Check that all script tables and the query tables of ComputeTableFunctions have timestamps
    Preconditions.checkArgument(Stream.concat(scriptTables.stream(),
        sqrlSchema.getFunctionStream(QueryTableFunction.class).map(
            QueryTableFunction::getQueryTable))
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

  private ResolvedExport exportTable(SQRLTable table, TableSink sink, RelBuilder relBuilder,
      SqrlFramework framework) {
    RelOptTable table1 = framework.getQueryPlanner().getCatalogReader()
            .getSqrlTable(table.getPath().stream().map(f->f.getDisplay()).collect(Collectors.toList()));
    relBuilder.scan(table1.getQualifiedName());
    List<RexNode> selects = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>();

    //todo: remove for latest columns
    AtomicInteger i = new AtomicInteger();
    table.getVisibleColumns().stream().forEach(c -> {
      selects.add(relBuilder.field(i.getAndIncrement()));//c.getVtName().getCanonical()));
      fieldNames.add(c.getName().getDisplay());
    });
    relBuilder.project(selects, fieldNames);
    return new ResolvedExport(table1.getQualifiedName().get(0), relBuilder.build(), sink);
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
