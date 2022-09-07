package ai.datasqrl.plan.global;

import ai.datasqrl.config.AbstractDAG;
import ai.datasqrl.plan.calcite.OptimizationStage;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.table.*;
import ai.datasqrl.plan.calcite.util.CalciteUtil;
import ai.datasqrl.plan.queries.APIQuery;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.tools.RelBuilder;

import java.util.*;
import java.util.stream.Stream;

public class DAGPlanner {

    public OptimizedDAG plan(CalciteSchema relSchema, Planner planner, Collection<APIQuery> queries) {

        List<QueryRelationalTable> queryTables = CalciteUtil.getTables(relSchema, QueryRelationalTable.class);
        Multimap<QueryRelationalTable,VirtualRelationalTable> toVirtual = HashMultimap.create();
        CalciteUtil.getTables(relSchema, VirtualRelationalTable.class).forEach(vt -> toVirtual.put(vt.getRoot().getBase(),vt));

        //Build the actual DAG
        LogicalDAG dag = LogicalDAG.of(queryTables, queries);
        dag = dag.trimToSinks(); //Remove unreachable parts of the DAG

        Map<StreamTableNode, MaterializationPreference> materialization = new HashMap<>();
        for (StreamTableNode tableNode : Iterables.filter(dag, StreamTableNode.class)) {
            QueryRelationalTable table = tableNode.table;
            //1. Optimize the logical plan and compute statistic
            optimizeTable(table);
            //2. Determine if we should materialize this table
            MaterializationPreference materialize = determineMaterialization(table);
            // make sure materialization strategy is compatible with inputs, else try to adjust
            Iterable<StreamTableNode> allinputs = Iterables.filter(dag.getAllInputsFromSource(tableNode, false), StreamTableNode.class);
            if (materialize == MaterializationPreference.MUST) {
                if (!Iterables.isEmpty(Iterables.filter(allinputs,t -> materialization.get(t) == MaterializationPreference.CANNOT))) {
                    throw new IllegalStateException("Incompatible materialization strategies");
                } else {
                    //Convert all inputs to "SHOULD"
                    Iterables.filter(allinputs, t-> materialization.get(t) == MaterializationPreference.SHOULD_NOT)
                            .forEach(t -> materialization.put(t, MaterializationPreference.SHOULD));
                }
            } else if (materialize == MaterializationPreference.SHOULD) {
                if (!Iterables.isEmpty(Iterables.filter(allinputs,t -> !materialization.get(t).isMaterialize()))) {
                    //At least one input should or can not be materialized, and hence neither should this table
                    materialize = MaterializationPreference.SHOULD_NOT;
                }
            }
            materialization.put(tableNode,materialize);
        }
        //3. If we don't materialize, input tables need to be persisted (i.e. determine where we cut the DAG)
        //   and if we do, then we need to set the flag on the QueryRelationalTable
        List<DBTableNode> nodes2Add = new ArrayList<>();
        for (StreamTableNode tableNode : Iterables.filter(dag, StreamTableNode.class)) {
            tableNode.table.getMatStrategy().setMaterialize(materialization.get(tableNode).isMaterialize());
            if (materialization.get(tableNode).isMaterialize()) {
                boolean isPersisted = dag.getOutputs(tableNode).stream().anyMatch(DBTableNode.class::isInstance);
                //If this node is materialized but some streamtable outputs aren't (i.e. they are computed in the database)
                //we need to persist this table and set a flag to indicate how to expand this table
                if (dag.getOutputs(tableNode).stream().filter(StreamTableNode.class::isInstance).map(DAGNode::asTable)
                        .anyMatch(n -> !materialization.get(n).isMaterialize())) {
                    VirtualRelationalTable vtable = Iterables.getOnlyElement(toVirtual.get(tableNode.table));
                    Preconditions.checkState(vtable.isRoot());
                    nodes2Add.add(new DBTableNode(vtable));
                    tableNode.table.getMatStrategy().setPersistedAs(vtable.getNameId());
                    isPersisted = true;
                }
                //Determine if we can postpone TopN inlining if table is persisted and not consumed by materialized nodes
                if (isPersisted && !tableNode.table.getPullups().isEmpty()) {
                    if (dag.getOutputs(tableNode).stream().filter(StreamTableNode.class::isInstance).map(DAGNode::asTable)
                            .allMatch(n -> !materialization.get(n).isMaterialize())) {
                        tableNode.table.getMatStrategy().setPullup(true);
                    }
                }
            }
        }
        dag = dag.addNodes(nodes2Add);

        //5. Expand tables using rules and produce one write-DAG
        //As a pre-processing step, make sure all timestamps are determined and imported tables are restructured accordingly
        for (StreamTableNode tableNode : Iterables.filter(dag.getSources(), StreamTableNode.class)) {
            Preconditions.checkArgument(tableNode.table instanceof ProxyImportRelationalTable);
            ProxyImportRelationalTable impTable = (ProxyImportRelationalTable) tableNode.table;
            impTable.getTimestamp().setBestTimestamp();
            ImportedSourceTable sourceTable = impTable.getSourceTable();
            //Set timestamp on source table
            int timestampIdx = impTable.getTimestamp().getTimestampIndex();
            int offset = timestampIdx - sourceTable.getBaseRowType().getFieldCount();
            AddedColumn.Simple timestampCol = null;
            if (offset<0) {
                sourceTable.setTimestampIndex(timestampIdx);
            } else {
                //Timestamp is an added column which means we have to remove it from impTable and re-arrange reamining simple
                //column before we update the rel node accordingly
                Preconditions.checkArgument(offset<impTable.getAddedFields().size(),"Invalid timestamp index");
                //TODO: This is a current limitation so we don't have to implement the re-ordering of fields when we
                //pull the timestamp column out (requires re-mapping field indexes for other added simple columns and putting projection on top to preserve original order
                Preconditions.checkArgument(offset==0, "Timestamp column must be added first");
                timestampCol = impTable.getAddedFields().get(offset);
                sourceTable.setTimestampColumn(timestampCol, planner.getTypeFactory());
            }
            //Rebuild relNode against sourceTabel
            RelBuilder relBuilder = planner.getRelBuilder();
            relBuilder.scan(sourceTable.getNameId());
            for (AddedColumn.Simple col : impTable.getAddedFields()) {
                if (col!=timestampCol) {
                    col.appendTo(relBuilder);
                }
            }
            impTable.setOptimizedRelNode(relBuilder.build());
        }
        //Validate every non-state table has a timestamp now
        Preconditions.checkState(Iterables.all(Iterables.transform(
                                    Iterables.filter(dag, StreamTableNode.class), t -> t.asTable().table),
                                    t -> t.getType()== TableType.STATE || t.getTimestamp().hasTimestamp()));

        //6. Produce an LP-tree for each query with all tables inlined and push down filters to determine indexes
        List<OptimizedDAG.WriteDB> writeDAG = new ArrayList<>();
        for (DBTableNode dbNode : Iterables.filter(dag, DBTableNode.class)) {
            if (dbNode.table.getRoot().getBase().getMatStrategy().isMaterialize()) {
                VirtualRelationalTable dbTable = dbNode.table;
                RelNode scanTable = planner.getRelBuilder().scan(dbTable.getNameId()).build();
                RelNode expanded = planner.transform(OptimizationStage.WRITE_DAG_EXPANSION,scanTable);
                writeDAG.add(new OptimizedDAG.WriteDB(dbTable,expanded));
            }
        }

        List<OptimizedDAG.ReadQuery> readDAG = new ArrayList<>();
        for (QueryNode qNode : Iterables.filter(dag, QueryNode.class)) {
            RelNode expanded = planner.transform(OptimizationStage.READ_DAG_EXPANSION,qNode.query.getRelNode());
            //TODO: Push down filters into queries to determine indexes needed on tables
            readDAG.add(new OptimizedDAG.ReadQuery(qNode.query,expanded));
        }

        return new OptimizedDAG(writeDAG,readDAG);
    }

    private void optimizeTable(QueryRelationalTable table) {
        //TODO: run volcano optimizer and get row estimate
        RelNode optimizedRel = table.getRelNode();
        table.setOptimizedRelNode(optimizedRel);
        table.setStatistic(TableStatistic.of(1));
        table.getPullups().forEach(p -> {
            //TODO: run volcano again for base rel
            RelNode optimized = p.getBaseRelNode();
            p.setOptimizedRelNode(optimized);
        });
    }

    private MaterializationPreference determineMaterialization(QueryRelationalTable table) {
        //TODO: implement based on following criteria:
        //- if imported table => MUST
        //- if subscription => MUST
        //- if hint provided => MUST or CANNOT depending on hint
        //- nested structure => MUST
        //- contains function that cannot be executed in database => MUST
        //- contains inner join where one side is high cardinality (with configurable threshold) => SHOULD NOT
        //- else SHOULD
        if (table instanceof ProxyImportRelationalTable) return MaterializationPreference.MUST;
        if (CalciteUtil.hasNesting(table.getRowType())) return MaterializationPreference.MUST;
        return MaterializationPreference.SHOULD;
    }

    private interface DAGNode extends AbstractDAG.Node {

        Stream<DAGNode> getInputs();

        default StreamTableNode asTable() {
            return null;
        }

    }

    @Value
    private static class StreamTableNode implements DAGNode {

        private final QueryRelationalTable table;

        private StreamTableNode(QueryRelationalTable table) {
            this.table = table;
        }

        @Override
        public Stream<DAGNode> getInputs() {
            if (table instanceof ProxyImportRelationalTable) return Stream.empty(); //imported tables have no inputs
            return VisitTableScans.findScanTables(table.getRelNode()).stream()
                    .map(t -> new StreamTableNode((QueryRelationalTable) t));
        }

        @Override
        public StreamTableNode asTable() {
            return this;
        }

        @Override
        public boolean isSink() {
            //TODO: return true if table is subscription
            return false;
        }
    }

    @Value
    private static class DBTableNode implements DAGNode {

        VirtualRelationalTable table;

        @Override
        public Stream<DAGNode> getInputs() {
            return Stream.of(new StreamTableNode(table.getRoot().getBase()));
        }
    }

    @Value
    private static class QueryNode implements DAGNode {

        private final APIQuery query;

        @Override
        public Stream<DAGNode> getInputs() {
            return VisitTableScans.findScanTables(query.getRelNode()).stream()
                    .map(t -> new DBTableNode((VirtualRelationalTable) t));
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

        public static LogicalDAG of(List<QueryRelationalTable> queryTables, Collection<APIQuery> queries) {
            Multimap<DAGNode, DAGNode> inputs = toInputs(queryTables.stream().map(t -> new StreamTableNode(t)));
            inputs.putAll(toInputs(queries.stream().map(q -> new QueryNode(q)).flatMap(qn -> Stream.concat(Stream.of(qn),qn.getInputs()))));
            return new LogicalDAG(inputs);
        }

        private static Multimap<DAGNode, DAGNode> toInputs(Stream<? extends DAGNode> nodes) {
            Multimap<DAGNode, DAGNode> inputs = HashMultimap.create();
            nodes.forEach( node -> {
                node.getInputs().forEach(input -> inputs.put(node,input));
            });
            return inputs;
        }

        public LogicalDAG addNodes(Collection<? extends DAGNode> nodes) {
            return addNodes(toInputs(nodes.stream()));
        }
    }


    private static class VisitTableScans extends RelShuttleImpl {

        final Set<AbstractRelationalTable> scanTables = new HashSet<>();

        public static Set<AbstractRelationalTable> findScanTables(@NonNull RelNode relNode) {
            VisitTableScans vts = new VisitTableScans();
            relNode.accept(vts);
            return vts.scanTables;
        }

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


}
