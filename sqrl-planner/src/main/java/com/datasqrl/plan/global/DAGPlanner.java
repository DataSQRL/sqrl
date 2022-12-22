/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.global;

import com.datasqrl.plan.global.OptimizedDAG.EngineSink;
import com.datasqrl.util.StreamUtil;
import com.datasqrl.name.Name;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.calcite.Planner;
import com.datasqrl.plan.calcite.hints.WatermarkHint;
import com.datasqrl.plan.calcite.rules.AnnotatedLP;
import com.datasqrl.plan.calcite.rules.SQRLLogicalPlanConverter;
import com.datasqrl.plan.calcite.table.*;
import com.datasqrl.plan.calcite.util.CalciteUtil;
import com.datasqrl.plan.local.generate.Resolve;
import com.datasqrl.plan.queries.APIQuery;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.datasqrl.plan.calcite.OptimizationStage.READ_DAG_STITCHING;
import static com.datasqrl.plan.calcite.OptimizationStage.WRITE_DAG_STITCHING;

/**
 * The DAGPlanner currently makes the simplifying assumption that the execution pipeline consists of
 * two stages: stream and database.
 */
public class DAGPlanner {

  private final Planner planner;
  private final ExecutionPipeline pipeline;

  private final ExecutionStage streamStage;
  private final ExecutionStage databaseStage;

  public DAGPlanner(Planner planner, ExecutionPipeline pipeline) {
    this.planner = planner;
    this.pipeline = pipeline;

    streamStage = pipeline.getStage(ExecutionEngine.Type.STREAM).get();
    databaseStage = pipeline.getStage(ExecutionEngine.Type.DATABASE).get();
  }

  public OptimizedDAG plan(CalciteSchema relSchema, Collection<APIQuery> queries,
      Collection<Resolve.ResolvedExport> exports) {

    List<QueryRelationalTable> queryTables = CalciteUtil.getTables(relSchema,
        QueryRelationalTable.class);

    //Assign timestamps to imports which should propagate and set all remaining timestamps
    StreamUtil.filterByClass(queryTables, ProxySourceRelationalTable.class)
        .forEach(this::finalizeSourceTable);
    Preconditions.checkArgument(queryTables.stream().allMatch(
        table -> !table.getType().hasTimestamp() || table.getTimestamp().hasFixedTimestamp()));

    //Plan API queries and find all tables that need to be materialized
    List<OptimizedDAG.ReadQuery> readDAG = new ArrayList<>();
    VisitTableScans tableScanVisitor = new VisitTableScans();
    for (APIQuery query : queries) {
      //Replace DEFAULT joins
      RelNode relNode = APIQueryRewriter.rewrite(planner.getRelBuilder(), query.getRelNode());
      //Rewrite query
      AnnotatedLP rewritten = SQRLLogicalPlanConverter.convert(relNode, getRelBuilderFactory(),
          SQRLLogicalPlanConverter.Config.builder()
              .startStage(databaseStage)
              .allowStageChange(false) //set to true once we can execute relnodes in the server
              .build());
      rewritten = rewritten.postProcess(getRelBuilderFactory().get(),
              query.getRelNode().getRowType().getFieldNames())
          .withDefaultSort().inlineSort(getRelBuilderFactory().get());
      assert rewritten.getPullups().isEmpty();
      relNode = rewritten.getRelNode();
      relNode = planner.transform(READ_DAG_STITCHING, relNode);
      relNode.accept(tableScanVisitor);
      readDAG.add(new OptimizedDAG.ReadQuery(query, relNode));
    }
    Preconditions.checkArgument(
        tableScanVisitor.scanTables.stream().allMatch(t -> t instanceof VirtualRelationalTable));
    Set<VirtualRelationalTable> tableSinks = StreamUtil.filterByClass(tableScanVisitor.scanTables,
        VirtualRelationalTable.class).collect(Collectors.toSet());

    //Fill all table sinks
    List<OptimizedDAG.WriteQuery> writeDAG = new ArrayList<>();
    //First, all the tables that need to be written to the database
    for (VirtualRelationalTable dbTable : tableSinks) {
      RelNode scanTable = planner.getRelBuilder().scan(dbTable.getNameId()).build();
      Pair<RelNode, Optional<Integer>> processed = processWriteVTable(scanTable, pipeline);
      RelNode processedRelnode = processed.getKey();
      Optional<Integer> timestampIdx = processed.getValue();
      //Check if we need to add timestamp column for nested child-tables
      if (!dbTable.isRoot() && timestampIdx.isPresent() &&
          dbTable.getRowType().getFieldCount() < processedRelnode.getRowType().getFieldCount()) {
        //Append timestamp to the end of table
        assert dbTable.getRowType().getFieldCount() == timestampIdx.get();
        ((VirtualRelationalTable.Child) dbTable).appendTimestampColumn(
            processedRelnode.getRowType().getFieldList().get(timestampIdx.get()),
            planner.getRelBuilder().getTypeFactory());
      }
      assert dbTable.getRowType().equals(processedRelnode.getRowType()) :
          "Rowtypes do not match: " + dbTable.getRowType() + " vs " + processedRelnode.getRowType();
      writeDAG.add(new OptimizedDAG.WriteQuery(
          new EngineSink(dbTable.getNameId(), dbTable.getNumPrimaryKeys(),
              dbTable.getRowType(), processed.getValue(), databaseStage),
          processed.getKey()));
    }
    //Second, all the tables that are exported
    int numExports = 1;
    for (Resolve.ResolvedExport export : exports) {
      RelNode relnode = export.getRelNode();
      Pair<RelNode, Optional<Integer>> processed = processWriteVTable(relnode, pipeline);
      RelNode processedRelnode = processed.getKey();
      //Pick only the selected keys
      RelBuilder relBuilder = planner.getRelBuilder().push(processedRelnode);
      relBuilder.project(relnode.getRowType().getFieldNames().stream()
          .map(n -> relBuilder.field(n)).collect(Collectors.toList()));
      processedRelnode = relBuilder.build();
      //TODO: check that the rowtype matches the schema of the tablesink (if present)
      String name = Name.addSuffix(export.getTable().getNameId(), String.valueOf(numExports++));
      writeDAG.add(new OptimizedDAG.WriteQuery(
          new OptimizedDAG.ExternalSink(name, export.getSink()),
          processedRelnode));
    }

    //Stitch together all StreamSourceTable
    for (StreamRelationalTableImpl sst : CalciteUtil.getTables(relSchema,
        StreamRelationalTableImpl.class)) {
      RelNode stitchedNode = planner.transform(WRITE_DAG_STITCHING, sst.getBaseRelation());
      sst.setBaseRelation(stitchedNode);
    }

//        readDAG = readDAG.stream().map( q -> {
//            RelNode newStitch = planner.transform(READ2WRITE_STITCHING, q.getRelNode());
//            return new OptimizedDAG.ReadQuery(q.getQuery(), newStitch);
//        }).collect(Collectors.toList());

    //Pick index structures for database tables based on the database queries
    IndexSelector indexSelector = new IndexSelector(planner,
        ((DatabaseEngine) databaseStage.getEngine()).getIndexSelectorConfig());
    Collection<IndexCall> indexCalls = readDAG.stream().map(indexSelector::getIndexSelection)
        .flatMap(List::stream).collect(Collectors.toList());
    Collection<IndexDefinition> indexDefinitions = indexSelector.optimizeIndexes(indexCalls)
        .keySet();

    return new OptimizedDAG(List.of(new OptimizedDAG.StagePlan(streamStage, writeDAG, null),
        new OptimizedDAG.StagePlan(databaseStage, readDAG, indexDefinitions)));
  }

  private Pair<RelNode, Optional<Integer>> processWriteVTable(RelNode scanTable,
      ExecutionPipeline pipeline) {
    //Shred the table if necessary before materialization
    AnnotatedLP processedRel = SQRLLogicalPlanConverter.convert(scanTable, getRelBuilderFactory(),
        SQRLLogicalPlanConverter.Config.builder()
            .startStage(streamStage)
            .allowStageChange(false)
            .build());
    processedRel = processedRel.postProcess(planner.getRelBuilder(),
        scanTable.getRowType().getFieldNames());
    RelNode expandedScan = processedRel.getRelNode();
    //Expand to full tree
    expandedScan = planner.transform(WRITE_DAG_STITCHING, expandedScan);
    //Determine if we need to append a timestamp for nested tables
    Optional<Integer> timestampIdx = processedRel.getType().hasTimestamp() ?
        Optional.of(processedRel.getTimestamp().getTimestampCandidate().getIndex())
        : Optional.empty();
    return Pair.of(expandedScan, timestampIdx);
  }

  private Supplier<RelBuilder> getRelBuilderFactory() {
    return () -> planner.getRelBuilder();
  }

  private void finalizeSourceTable(ProxySourceRelationalTable table) {
    // Determine timestamp
    if (!table.getTimestamp().hasFixedTimestamp()) {
      table.getTimestamp().getBestCandidate().fixAsTimestamp();
    }
    // Rewrite LogicalValues to TableScan and add watermark hint
    new SourceTableRewriter(table, planner.getRelBuilder()).replaceImport();
  }

  private static class VisitTableScans extends RelShuttleImpl {

    final Set<AbstractRelationalTable> scanTables = new HashSet<>();

    @Override
    public RelNode visit(TableScan scan) {
      QueryRelationalTable table = scan.getTable().unwrap(QueryRelationalTable.class);
      if (table == null) { //It's a database query
        scanTables.add(scan.getTable().unwrap(VirtualRelationalTable.class));
      } else {
        scanTables.add(table);
      }
      return super.visit(scan);
    }
  }

  /**
   * Replaces LogicalValues with the TableScan for the actual import table
   */
  @AllArgsConstructor
  private static class SourceTableRewriter extends RelShuttleImpl {

    final ProxySourceRelationalTable table;
    final RelBuilder relBuilder;

    public void replaceImport() {
      RelNode updated = table.getRelNode().accept(this);
      int timestampIdx = table.getTimestamp().getTimestampCandidate().getIndex();
      Preconditions.checkArgument(timestampIdx < updated.getRowType().getFieldCount());
      WatermarkHint watermarkHint = new WatermarkHint(timestampIdx);
      updated = ((Hintable) updated).attachHints(List.of(watermarkHint.getHint()));
      table.updateRelNode(updated);
    }

    @Override
    public RelNode visit(LogicalValues values) {
      //The Values are a place-holder for the tablescan, replace with actual table now
      return relBuilder.scan(table.getBaseTable().getNameId()).build();
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
