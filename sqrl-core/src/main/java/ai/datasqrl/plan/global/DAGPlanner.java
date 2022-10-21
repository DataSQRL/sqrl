package ai.datasqrl.plan.global;

import ai.datasqrl.config.util.StreamUtil;
import ai.datasqrl.physical.ExecutionEngine;
import ai.datasqrl.physical.pipeline.ExecutionPipeline;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.hints.WatermarkHint;
import ai.datasqrl.plan.calcite.rules.AnnotatedLP;
import ai.datasqrl.plan.calcite.rules.SQRLLogicalPlanConverter;
import ai.datasqrl.plan.calcite.table.AbstractRelationalTable;
import ai.datasqrl.plan.calcite.table.ProxyImportRelationalTable;
import ai.datasqrl.plan.calcite.table.QueryRelationalTable;
import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.calcite.util.CalciteUtil;
import ai.datasqrl.plan.queries.APIQuery;
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

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static ai.datasqrl.plan.calcite.OptimizationStage.READ_DAG_STITCHING;
import static ai.datasqrl.plan.calcite.OptimizationStage.WRITE_DAG_STITCHING;

@AllArgsConstructor
public class DAGPlanner {

    private final Planner planner;

    public OptimizedDAG plan(CalciteSchema relSchema, Collection<APIQuery> queries, ExecutionPipeline pipeline) {

        List<QueryRelationalTable> queryTables = CalciteUtil.getTables(relSchema, QueryRelationalTable.class);

        //Assign timestamps to imports which should propagate and set all remaining timestamps
        StreamUtil.filterByClass(queryTables, ProxyImportRelationalTable.class).forEach(this::finalizeImportTable);
        Preconditions.checkArgument(queryTables.stream().allMatch(table -> !table.getType().hasTimestamp() || table.getTimestamp().hasFixedTimestamp()));

        //Plan API queries and find all tables that need to be materialized
        List<OptimizedDAG.ReadQuery> readDAG = new ArrayList<>();
        VisitTableScans tableScanVisitor = new VisitTableScans();
        for (APIQuery query : queries) {
            //Replace DEFAULT joins
            RelNode relNode = APIQueryRewriter.rewrite(planner.getRelBuilder(),query.getRelNode());
            //Rewrite query
            AnnotatedLP rewritten = SQRLLogicalPlanConverter.convert(relNode, pipeline.getStage(ExecutionEngine.Type.DATABASE).get(),
                    getRelBuilderFactory());
            relNode = rewritten.getRelNode();
            relNode = planner.transform(READ_DAG_STITCHING,relNode);
            relNode.accept(tableScanVisitor);
            //TODO: Push down filters into queries to determine indexes needed on tables
            readDAG.add(new OptimizedDAG.ReadQuery(query,relNode));
        }
        Preconditions.checkArgument(tableScanVisitor.scanTables.stream().allMatch(t -> t instanceof VirtualRelationalTable));
        Set<VirtualRelationalTable> tableSinks = StreamUtil.filterByClass(tableScanVisitor.scanTables,VirtualRelationalTable.class).collect(Collectors.toSet());


        List<OptimizedDAG.MaterializeQuery> writeDAG = new ArrayList<>();
        for (VirtualRelationalTable dbTable : tableSinks) {
            RelNode scanTable = planner.getRelBuilder().scan(dbTable.getNameId()).build();
            //Shred the table if necessary before materialization
            AnnotatedLP processedRel = SQRLLogicalPlanConverter.convert(scanTable,pipeline.getStage(ExecutionEngine.Type.STREAM).get(),
                    getRelBuilderFactory());
            processedRel = processedRel.postProcess(planner.getRelBuilder(), dbTable.getRowType().getFieldNames());
            RelNode expandedScan = processedRel.getRelNode();
            //Expand to full tree
            expandedScan = planner.transform(WRITE_DAG_STITCHING,expandedScan);
            Optional<Integer> timestampIdx = processedRel.getType().hasTimestamp()?
                    Optional.of(processedRel.getTimestamp().getTimestampCandidate().getIndex()):Optional.empty();
            if (!dbTable.isRoot() && timestampIdx.isPresent()) {
                //Append timestamp to the end of table columns
                assert dbTable.getRowType().getFieldCount()==timestampIdx.get();
                ((VirtualRelationalTable.Child)dbTable).appendTimestampColumn(expandedScan.getRowType().getFieldList().get(timestampIdx.get()),
                        planner.getRelBuilder().getTypeFactory());
            }
            assert dbTable.getRowType().equals(expandedScan.getRowType()) :
                    "Rowtypes do not match: " + dbTable.getRowType() + " vs " + expandedScan.getRowType();
            writeDAG.add(new OptimizedDAG.MaterializeQuery(
                    new OptimizedDAG.TableSink(dbTable, timestampIdx),
                    expandedScan));
        }

//        readDAG = readDAG.stream().map( q -> {
//            RelNode newStitch = planner.transform(READ2WRITE_STITCHING, q.getRelNode());
//            return new OptimizedDAG.ReadQuery(q.getQuery(), newStitch);
//        }).collect(Collectors.toList());

        return new OptimizedDAG(writeDAG,readDAG);
    }

    private Supplier<RelBuilder> getRelBuilderFactory() {
        return () -> planner.getRelBuilder();
    }

    private void finalizeImportTable(ProxyImportRelationalTable table) {
        // Determine timestamp
        if (!table.getTimestamp().hasFixedTimestamp()) {
            table.getTimestamp().getBestCandidate().fixAsTimestamp();
        }
        // Rewrite LogicalValues to TableScan and add watermark hint
        new ImportTableRewriter(table,planner.getRelBuilder()).replaceImport();
    }

    private static class VisitTableScans extends RelShuttleImpl {

        final Set<AbstractRelationalTable> scanTables = new HashSet<>();

        @Override
        public RelNode visit(TableScan scan) {
            QueryRelationalTable table = scan.getTable().unwrap(QueryRelationalTable.class);
            if (table==null) { //It's a database query
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
    private static class ImportTableRewriter extends RelShuttleImpl {

        final ProxyImportRelationalTable table;
        final RelBuilder relBuilder;

        public void replaceImport() {
            RelNode updated = table.getRelNode().accept(this);
            int timestampIdx = table.getTimestamp().getTimestampCandidate().getIndex();
            Preconditions.checkArgument(timestampIdx<updated.getRowType().getFieldCount());
            WatermarkHint watermarkHint = new WatermarkHint(timestampIdx);
            updated = ((Hintable)updated).attachHints(List.of(watermarkHint.getHint()));
            table.updateRelNode(updated);
        }

        @Override
        public RelNode visit(LogicalValues values) {
            //The Values are a place-holder for the tablescan, replace with actual table now
            return relBuilder.scan(table.getSourceTable().getNameId()).build();
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
            if (join.getJoinType()== JoinRelType.DEFAULT) { //replace DEFAULT joins with INNER
                join = join.copy(join.getTraitSet(),join.getCondition(),join.getLeft(),join.getRight(),JoinRelType.INNER,join.isSemiJoinDone());
            }
            return super.visit(join);
        }

    }

}
