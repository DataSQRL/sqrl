package ai.datasqrl.plan.global;

import ai.datasqrl.config.AbstractDAG;
import ai.datasqrl.plan.calcite.table.ImportedRelationalTable;
import ai.datasqrl.plan.calcite.table.QueryRelationalTable;
import ai.datasqrl.plan.calcite.table.TableStatistic;
import ai.datasqrl.plan.calcite.table.TopNRelationalTable;
import ai.datasqrl.plan.queries.APIQuery;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

public class DAGPlanner {

    public OptimizedDAG plan(Collection<QueryRelationalTable> tables, Collection<APIQuery> queries) {
        //Build the actual DAG
        LogicalDAG dag = LogicalDAG.of(Stream.concat(tables.stream().map(t -> new TableDAGNode(t)),
                                                     queries.stream().map(q -> new QueryDAGNode(q))));
        dag = dag.trimToSinks(); //Remove unreachable parts of the DAG

        for (TableDAGNode tableNode : Iterables.filter(dag,TableDAGNode.class)) {
            QueryRelationalTable table = tableNode.table;
            //1. Optimize the logical plan and compute statistic
            optimizeTable(table);
            //2. Determine if we should materialize this table
            tableNode.materialize = determineMaterialization(tableNode.table);
            // make sure materialization strategy is compatible with inputs, else try to adjust
            Iterable<TableDAGNode> allinputs = Iterables.filter(dag.getAllInputsFromSource(tableNode),TableDAGNode.class);
            if (tableNode.materialize==MaterializationStrategy.MUST) {
                if (Iterables.filter(allinputs,t -> t.materialize==MaterializationStrategy.CANNOT).iterator().hasNext()) {
                    throw new IllegalStateException("Incompatible materialization strategies");
                } else {
                    //Convert all inputs to "SHOULD"
                    Iterables.filter(allinputs, t-> t.materialize==MaterializationStrategy.SHOULD_NOT)
                            .forEach(t -> t.materialize=MaterializationStrategy.SHOULD);
                }
            } else if (tableNode.materialize==MaterializationStrategy.SHOULD) {
                if (Iterables.filter(allinputs,t -> !t.materialize.isMaterialize()).iterator().hasNext()) {
                    //At least one input should or can not be materialized, and hence neither should this table
                    tableNode.materialize = MaterializationStrategy.SHOULD_NOT;
                }
            }
        }
        //3. If we don't materialize, input tables need to be persisted (i.e. determine where we cut the DAG)
        //   and if we do, then we need to set the flag on the QueryRelationalTable
        for (TableDAGNode tableNode : Iterables.filter(dag,TableDAGNode.class)) {
            if (!tableNode.materialize.isMaterialize()) {
                dag.getInputs(tableNode).stream().forEach(i -> {
                    if (i.asTable().materialize.isMaterialize()) i.asTable().persisted = true;
                });
            } else {
                tableNode.table.setMaterialize(true);
            }
        }
        //4. Determine if we can postpone TopN inlining if table is persisted and not consumed by materialized nodes
        for (TableDAGNode tableNode : Iterables.filter(dag,TableDAGNode.class)) {
            if (!tableNode.persisted || !(tableNode.table instanceof TopNRelationalTable)) continue;
            TopNRelationalTable table = (TopNRelationalTable)tableNode.table;
            if (dag.getOutputs(tableNode).stream().allMatch(i -> i.asTable()==null
                    || !i.asTable().materialize.isMaterialize())) {
                //All tables that consume this table are computed in the database, hence it is more efficient
                //to compute the topN constraint for this table in the database as well
                table.setInlinedTopN(false);
            }
        }
        //5. Expand tables using rules and produce one write-DAG
        //As a pre-processing step, make sure all timestamps are determined and imported tables are restructured accordingly
        for (TableDAGNode tableNode : Iterables.filter(dag.getSources(),TableDAGNode.class)) {
            Preconditions.checkArgument(tableNode.table instanceof ImportedRelationalTable);
            ImportedRelationalTable impTable = (ImportedRelationalTable) tableNode.table;
            impTable.getTimestamp().setBestTimestamp();
            //TODO: replace with table that can be used by flink
        }
        //Validate every non-state table has a timestamp now
        Preconditions.checkState(Iterables.all(Iterables.transform(
                                    Iterables.filter(dag,TableDAGNode.class),t -> t.asTable().table),
                                    t -> t.getType()== QueryRelationalTable.Type.STATE || t.getTimestamp().hasTimestamp()));


        //6. Produce an LP-tree for each query with all tables inlined and push down filters to determine indexes

        //TODO: Push down filters into queries to determine indexes needed on tables
        return null;
    }

    private void optimizeTable(QueryRelationalTable table) {
        //TODO: run volcano optimizer and get row estimate
        RelNode optimizedRel = table.getRelNode();
        table.setOptimizedRelNode(optimizedRel);
        table.setStatistic(TableStatistic.of(1));
        if (table instanceof TopNRelationalTable) {
            TopNRelationalTable topNtable = (TopNRelationalTable) table;
            //TODO: run volcano again for base rel
            optimizedRel = topNtable.getBaseRel();
            topNtable.setOptimizedBaseRel(optimizedRel);
        }
    }

    private MaterializationStrategy determineMaterialization(QueryRelationalTable table) {
        //TODO: implement based on following criteria:
        //- if hint provided => MUST or CANNOT depending on hint
        //- if imported table => MUST
        //- if subscription => MUST
        //- contains function that cannot be executed in database => MUST
        //- contains inner join where one side is high cardinality (with configurable threshold) => SHOULD NOT
        //- else SHOULD
        if (table instanceof ImportedRelationalTable) return MaterializationStrategy.MUST;
        return MaterializationStrategy.SHOULD;
    }

    private interface DAGNode extends AbstractDAG.Node {

        RelNode getRelNode();

        default TableDAGNode asTable() {
            return null;
        }

    }

    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    private static class TableDAGNode implements DAGNode {

        @EqualsAndHashCode.Include
        private final QueryRelationalTable table;
        private MaterializationStrategy materialize;
        private boolean persisted = false;

        private TableDAGNode(QueryRelationalTable table) {
            this.table = table;
        }

        @Override
        public RelNode getRelNode() {
            if (table instanceof ImportedRelationalTable) { //imported tables have no inputs
                return null;
            } else {
                return table.getRelNode();
            }
        }

        @Override
        public TableDAGNode asTable() {
            return this;
        }

        @Override
        public boolean isSink() {
            //TODO: return true if table is subscription
            return false;
        }
    }

    @Value
    private static class QueryDAGNode implements DAGNode {

        private final APIQuery query;

        @Override
        public RelNode getRelNode() {
            return query.getRelNode();
        }

        @Override
        public boolean isSink() {
            return true;
        }
    }

    private static class LogicalDAG extends AbstractDAG<DAGNode, LogicalDAG> {

        protected LogicalDAG(Multimap<DAGNode, DAGNode> inputs) {
            super(inputs);
        }

        @Override
        protected LogicalDAG create(Multimap<DAGNode, DAGNode> inputs) {
            return new LogicalDAG(inputs);
        }

        public static LogicalDAG of(Stream<DAGNode> nodes) {
            Multimap<DAGNode, DAGNode> inputs = HashMultimap.create();
            nodes.forEach( node -> {
                Set<QueryRelationalTable> scanTables = Collections.EMPTY_SET;
                RelNode relNode = node.getRelNode();
                if (relNode!=null) {
                    scanTables = VisitTableScans.findScanTables(relNode);
                }
                scanTables.forEach(t -> inputs.put(node,new TableDAGNode(t)));
            });
            return new LogicalDAG(inputs);
        }
    }


    private static class VisitTableScans extends RelShuttleImpl {

        final Set<QueryRelationalTable> scanTables = new HashSet<QueryRelationalTable>();

        public static Set<QueryRelationalTable> findScanTables(@NonNull RelNode relNode) {
            VisitTableScans vts = new VisitTableScans();
            relNode.accept(vts);
            return vts.scanTables;
        }

        @Override
        public RelNode visit(TableScan scan) {
            QueryRelationalTable table = scan.getTable().unwrap(QueryRelationalTable.class);
            scanTables.add(table);
            return super.visit(scan);
        }
    }


}
