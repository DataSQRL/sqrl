package ai.datasqrl.plan.calcite.rules;

import ai.datasqrl.plan.calcite.hints.TimeAggregationHint;
import ai.datasqrl.plan.calcite.table.*;
import ai.datasqrl.plan.calcite.util.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import lombok.Value;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Value
public class SQRLLogicalPlanConverter extends AbstractSqrlRelShuttle<SQRLLogicalPlanConverter.ProcessedRel> {

    public final Supplier<RelBuilder> relBuilderFactory;
    public final SqrlRexUtil rexUtil;

    @Value
    public class ProcessedRel implements RelHolder {

        RelNode relNode;
        TableType type;
        ContinuousIndexMap primaryKey;
        TimestampHolder.Derived timestamp;
        ContinuousIndexMap indexMap;

        List<JoinTable> joinTables;

        NowFilter nowFilter;
        TopNConstraint topN;

        /**
         * Called to inline the TopNConstraint on top of the input relation
         * @return
         */
        public ProcessedRel inlineTopN() {
            if (topN.isEmpty()) return this;
            return inlinePullups(); //Inlining topN requires inlining nowFilter first
        }

        public ProcessedRel inlineNowFilter() {
            if (nowFilter.isEmpty()) return this;
            throw new UnsupportedOperationException("Not yet implemented");
        }

        public boolean hasPullups() {
            return !topN.isEmpty() || !nowFilter.isEmpty();
        }

        public List<DatabasePullup> getPullups() {
            List<DatabasePullup> pullups = new ArrayList<>();
            if (!nowFilter.isEmpty()) pullups.add(nowFilter);
            if (!topN.isEmpty()) pullups.add(topN);
            return pullups;
        }

        public ProcessedRel inlinePullups() {
            Preconditions.checkArgument(!hasPullups(), "not yet supported");
            return this;
        }
    }

    /**
     * Moves the primary key columns to the front and adds projection to only return
     * columns that the user selected, are part of the primary key, or a timestamp candidate
     *
     * @param input
     * @return
     */
    public ProcessedRel postProcess(ProcessedRel input) {
        ContinuousIndexMap indexMap = input.indexMap;
        HashMap<Integer,Integer> remapping = new HashMap<>();
        int index = 0;
        for (int i = 0; i < input.primaryKey.getSourceLength(); i++) {
            remapping.put(input.primaryKey.map(i),index++);
        }
        for (int i = 0; i < input.indexMap.getSourceLength(); i++) {
            int target = input.indexMap.map(i);
            if (!remapping.containsKey(target)) {
                remapping.put(target,index++);
            }
        }
        for (TimestampHolder.Candidate c : input.timestamp.getCandidates()) {
            if (!remapping.containsKey(c.getIndex())) {
                remapping.put(c.getIndex(),index++);
            }
        }

        Preconditions.checkArgument(index<=indexMap.getTargetLength() && remapping.size() == index);
        List<RexNode> projects = new ArrayList<>(indexMap.getTargetLength());
        remapping.entrySet().stream().map(e -> new IndexMap.Pair(e.getKey(),e.getValue()))
                .sorted((a, b)-> Integer.compare(a.getTarget(),b.getTarget()))
                .forEach(p -> {
            projects.add(p.getTarget(),RexInputRef.of(p.getSource(),
                    input.relNode.getRowType()));
        });
        IndexMap remap = IndexMap.of(remapping);
        RelBuilder relBuilder = relBuilderFactory.get();
        relBuilder.push(input.relNode);
        relBuilder.project(projects);

        return new ProcessedRel(relBuilder.build(),input.type,input.primaryKey.remap(remap),
                input.timestamp.remapIndexes(remap), input.indexMap.remap(remap), null,
                input.nowFilter.remap(remap), input.topN.remap(remap));

    }


    private ProcessedRel extractTopNConstraint(ProcessedRel input, LogicalProject project) {
        /*
        TODO: Detect if this is a distinct/top-n pattern and pull out as TopNConstraint
        Look for project#2-filter-project#1 pattern where:
        - project#1 preserves the input fields and adds 1-3 window fields that have the same partition and order (extract both)
        - filter constrains those window fields only in a topN + distinct pattern (extract both)
        - project#2 removes the added window fields and preserves the original input fields (can discard)

        Check if we are partitioning by the primary key of the incoming relation - if so, we can remove the entire
        construct since partitioning on a primary key must always yield just one row (which is trivially topN (for N>0), sorted, and distinct)
	    This requires that we are smart about determining primary keys for joins: if we join A x B on an equality
	    condition that constrains all primary key columns of B by columns of A, then the resulting primary key is
	    just the primary key of A (without B appended).
         */
        return input;
    }

    @Override
    public RelNode visit(TableScan tableScan) {
        //The base scan tables for all SQRL queries are VirtualRelationalTable
        VirtualRelationalTable vtable = tableScan.getTable().unwrap(VirtualRelationalTable.class);
        Preconditions.checkArgument(vtable != null);

        //Shred the virtual table all the way to root:
        //First, we prepare all the data structures
        ContinuousIndexMap.Builder indexMap = ContinuousIndexMap.builder(vtable.getNumColumns());
        List<JoinTable> joinTables = new ArrayList<>();
        ContinuousIndexMap.Builder primaryKey = ContinuousIndexMap.builder(vtable.getNumPrimaryKeys());
        //Now, we shred
        RelNode relNode = shredTable(vtable, primaryKey, indexMap, joinTables, true).build();
        //Finally, we assemble the result
        VirtualRelationalTable.Root root = vtable.getRoot();
        QueryRelationalTable queryTable = root.getBase();
        int mapToLength = relNode.getRowType().getFieldCount();
        ProcessedRel result = new ProcessedRel(relNode, queryTable.getType(),
                primaryKey.build(mapToLength),
                new TimestampHolder.Derived(queryTable.getTimestamp()),
                indexMap.build(mapToLength), joinTables, NowFilter.EMPTY, TopNConstraint.EMPTY);
        return setRelHolder(result);
    }

    private RelBuilder shredTable(VirtualRelationalTable vtable, ContinuousIndexMap.Builder primaryKey,
                                  ContinuousIndexMap.Builder indexMap, List<JoinTable> joinTables,
                                  boolean isLeaf) {
        Preconditions.checkArgument(joinTables.isEmpty());
        return shredTable(vtable, primaryKey, indexMap, joinTables, null, isLeaf);
    }

    private RelBuilder shredTable(VirtualRelationalTable vtable, ContinuousIndexMap.Builder primaryKey,
                                  List<JoinTable> joinTables, Pair<JoinTable,RelBuilder> startingBase) {
        Preconditions.checkArgument(joinTables.isEmpty());
        return shredTable(vtable, primaryKey, null, joinTables, startingBase, false);
    }

    private RelBuilder shredTable(VirtualRelationalTable vtable, ContinuousIndexMap.Builder primaryKey,
                                  ContinuousIndexMap.Builder indexMap, List<JoinTable> joinTables,
                                  Pair<JoinTable,RelBuilder> startingBase, boolean isLeaf) {
        RelBuilder builder;
        List<AddedColumn> columns2Add;
        int offset;
        JoinTable joinTable;
        if (startingBase!=null && startingBase.getKey().getTable().equals(vtable)) {
            builder = startingBase.getValue();
            joinTables.add(startingBase.getKey());
            return builder;
        }
        if (vtable.isRoot()) {
            VirtualRelationalTable.Root root = (VirtualRelationalTable.Root) vtable;
            offset = 0;
            builder = relBuilderFactory.get();
            builder.scan(root.getBase().getNameId());
            CalciteUtil.addIdentityProjection(builder,root.getNumQueryColumns());
            joinTable = JoinTable.ofRoot(root);
            columns2Add = vtable.getAddedColumns().stream()
                    .filter(Predicate.not(AddedColumn::isInlined))
                    .collect(Collectors.toList());
        } else {
            VirtualRelationalTable.Child child = (VirtualRelationalTable.Child) vtable;
            builder = shredTable(child.getParent(), primaryKey, indexMap, joinTables, startingBase,false);
            JoinTable parentJoinTable = Iterables.getLast(joinTables);
            int indexOfShredField = parentJoinTable.getOffset() + child.getShredIndex();
            CorrelationId id = new CorrelationId(0);
            RelDataType base = builder.peek().getRowType();
            offset = base.getFieldCount();

            builder
                    .values(List.of(List.of(builder.getRexBuilder().makeExactLiteral(BigDecimal.ZERO))),
                            new RelRecordType(List.of(new RelDataTypeFieldImpl(
                                    "ZERO",
                                    0,
                                    builder.getTypeFactory().createSqlType(SqlTypeName.INTEGER)))))
                    .project(
                            List.of(builder.getRexBuilder()
                                    .makeFieldAccess(
                                            builder.getRexBuilder().makeCorrel(base, id),
                                            indexOfShredField)))
                    .uncollect(List.of(), false)
                    .correlate(JoinRelType.INNER, id, RexInputRef.of(indexOfShredField,  base));
            joinTable = new JoinTable(vtable, parentJoinTable, offset);
            columns2Add = vtable.getAddedColumns();
        }
        for (int i = 0; i < vtable.getNumLocalPks(); i++) {
            primaryKey.add(offset+i);
            if (!isLeaf && startingBase==null) indexMap.add(offset+i);
        }
        if (isLeaf && startingBase==null) { //Construct indexMap
            //All non-nested fields are part of the virtual table row type
            List<RelDataTypeField> queryRowType = vtable.getQueryRowType().getFieldList();
            for (int i = 0; i < queryRowType.size(); i++) {
                RelDataTypeField field = queryRowType.get(i);
                if (!CalciteUtil.isNestedTable(field.getType())) {
                    indexMap.add(offset+i);
                }
            }
        }
        //Add additional columns
        JoinTable.Path path = JoinTable.Path.of(joinTable);
        for (AddedColumn column : columns2Add) {
            List<RexNode> projects = rexUtil.getIdentityProject(builder.peek());
            RexNode added;
            if (column instanceof AddedColumn.Simple) {
                added = ((AddedColumn.Simple) column).getExpression(path);
            } else {
                AddedColumn.Complex cc = (AddedColumn.Complex)column;
                //TODO: Need to join and project out everything but the last column
                throw new UnsupportedOperationException("Not yet implemented");
            }
            projects.add(added);
            builder.project(projects);
        }
        joinTables.add(joinTable);
        return builder;
    }

    private static final SqrlRexUtil.RexFinder FIND_NOW = SqrlRexUtil.findFunctionByName("now");

    @Override
    public RelNode visit(LogicalFilter logicalFilter) {
        ProcessedRel input = getRelHolder(logicalFilter.getInput().accept(this));
        if (input.topN.hasLimit()) input = input.inlineTopN(); //Filtering doesn't preserve limits
        RexNode condition = logicalFilter.getCondition();
        condition = SqrlRexUtil.mapIndexes(condition,input.indexMap);
        //Check if it has a now() predicate and pull out or throw an exception if malformed
        LogicalFilter filter;
        TimestampHolder.Derived timestamp = input.timestamp;
        if (FIND_NOW.contains(condition)) {
            //TODO: redo this part
            RelBuilder builder = relBuilderFactory.get();
            List<RexNode> conjunctions = rexUtil.getConjunctions(condition);
            List<RexNode> nowConjunctions = new ArrayList<>();
            Optional<Integer> timestampIndex = Optional.empty();
            Iterator<RexNode> iter = conjunctions.iterator();
            while (iter.hasNext()) {
                RexNode conjunction = iter.next();
                Optional<Integer> tsi = getRecencyFilterTimestampIndex(conjunction);
                if (tsi.isPresent() && timestamp.isCandidate(tsi.get()) &&
                        (timestampIndex.isEmpty() || timestampIndex.get().equals(tsi.get()))) {
                    timestampIndex = tsi;
                    nowConjunctions.add(condition);
                    iter.remove();
                }
            }

            if (timestampIndex.isPresent()) {
                //Break out now() filter with hint and set timestamp
                filter = logicalFilter.copy(logicalFilter.getTraitSet(),logicalFilter.getInput(), RexUtil.composeConjunction(builder.getRexBuilder(), conjunctions));
                filter = logicalFilter.copy(logicalFilter.getTraitSet(),filter,RexUtil.composeConjunction(builder.getRexBuilder(), nowConjunctions));
                //TODO: upgrade Calcite to make this possible
                //filter = filter.withHints(List.of(SqrlHints.recencyFilter()));
                timestamp = timestamp.fixTimestamp(timestampIndex.get());
            } else {
                throw new IllegalArgumentException("");
            }
        } else {
            filter = logicalFilter.copy(logicalFilter.getTraitSet(),logicalFilter.getInput(),condition);
        }
        return setRelHolder(new ProcessedRel(filter,input.type,input.primaryKey,
                timestamp,input.indexMap,input.joinTables, input.nowFilter, input.topN));
    }

    private static Optional<Integer> getRecencyFilterTimestampIndex(RexNode condition) {
        //TODO: implement, reuse Flink code to determine interval
        return Optional.empty();
    }


    @Override
    public RelNode visit(LogicalProject logicalProject) {
        ProcessedRel rawInput = getRelHolder(logicalProject.getInput().accept(this));
        rawInput = extractTopNConstraint(rawInput, logicalProject);


        ContinuousIndexMap trivialMap = getTrivialMapping(logicalProject, rawInput.indexMap);
        if (trivialMap!=null) {
            //If it's a trivial project, we remove it and only update the indexMap
            return setRelHolder(new ProcessedRel(rawInput.relNode,rawInput.type,rawInput.primaryKey, rawInput.timestamp,
                    trivialMap, rawInput.joinTables, rawInput.nowFilter, rawInput.topN));
        }
        ProcessedRel input = rawInput.inlineTopN();
        Preconditions.checkArgument(input.topN.isEmpty());
        //Update index mappings
        List<RexNode> updatedProjects = new ArrayList<>();
        Multimap<Integer,Integer> mappedProjects = HashMultimap.create();
        List<TimestampHolder.Candidate> timeCandidates = new ArrayList<>();
        for (Ord<RexNode> exp : Ord.<RexNode>zip(logicalProject.getProjects())) {
            RexNode mapRex = SqrlRexUtil.mapIndexes(exp.e,input.indexMap);
            updatedProjects.add(exp.i,mapRex);
            if (mapRex instanceof RexInputRef) {
                int index = (((RexInputRef) mapRex)).getIndex();
                mappedProjects.put(index,exp.i);
            }
            //Check for preserved timestamps
            rexUtil.getPreservedTimestamp(mapRex, input.timestamp)
                    .map(candidate -> timeCandidates.add(candidate.withIndex(exp.i)));
        }
        //Make sure we pull the primary keys and timestamp candidates through (i.e. append those to the projects
        //if not already present)
        ContinuousIndexMap.Builder primaryKey = ContinuousIndexMap.builder(input.primaryKey.getSourceLength());
        input.primaryKey.getMapping().forEach(p -> {
            Collection<Integer> target = mappedProjects.get(p.getTarget());
            if (target.size()>1) throw new IllegalArgumentException("Cannot select a primary key column multiple times");
            else if (target.size()==1) primaryKey.add(Iterables.getOnlyElement(target));
            else {
                //Need to add it
                int index = updatedProjects.size();
                updatedProjects.add(index,RexInputRef.of(p.getTarget(),input.relNode.getRowType()));
                primaryKey.add(index);
            }
        });
        for (TimestampHolder.Candidate candidate : input.timestamp.getCandidates()) {
            //Check if candidate is already mapped through timestamp preserving function
            if (timeCandidates.stream().anyMatch(c -> c.getId() == candidate.getId())) continue;
            Collection<Integer> target = mappedProjects.get(candidate.getIndex());
            if (target.isEmpty()) {
                //Need to add candidate
                int index = updatedProjects.size();
                updatedProjects.add(index,RexInputRef.of(candidate.getIndex(),input.relNode.getRowType()));
                timeCandidates.add(candidate.withIndex(index));
            } else {
                target.forEach(t -> timeCandidates.add(candidate.withIndex(t)));
            }
        }
        TimestampHolder.Derived timestamp = input.timestamp.propagate(timeCandidates);
        //TODO: need to ensure pullup columns are preseved!!


        //Build new project
        RelBuilder relB = relBuilderFactory.get();
        relB.push(input.relNode);
        relB.project(updatedProjects);
        relB.hints(logicalProject.getHints());
        RelNode newProject = relB.build();
        int fieldCount = updatedProjects.size();
        return setRelHolder(new ProcessedRel(newProject,input.type,primaryKey.build(fieldCount),
                timestamp, ContinuousIndexMap.identity(logicalProject.getProjects().size(),fieldCount),null,
                input.nowFilter, input.topN));
    }

    private ContinuousIndexMap getTrivialMapping(LogicalProject project, ContinuousIndexMap baseMap) {
        ContinuousIndexMap.Builder b = ContinuousIndexMap.builder(project.getProjects().size());
        for (RexNode rex : project.getProjects()) {
            if (!(rex instanceof RexInputRef)) return null;
            b.add(baseMap.map((((RexInputRef) rex)).getIndex()));
        }
        return b.build(baseMap.getTargetLength());
    }

    @Override
    public RelNode visit(LogicalJoin logicalJoin) {
        ProcessedRel leftInput = getRelHolder(logicalJoin.getLeft().accept(this));
        ProcessedRel rightInput = getRelHolder(logicalJoin.getRight().accept(this));
        JoinRelType joinType = logicalJoin.getJoinType();


        ContinuousIndexMap joinedIndexMap = leftInput.indexMap.join(rightInput.indexMap);
        RexNode condition = SqrlRexUtil.mapIndexes(logicalJoin.getCondition(),joinedIndexMap);
        SqrlRexUtil.EqualityComparisonDecomposition eqDecomp = rexUtil.decomposeEqualityComparison(condition);

        //Identify if this is an identical self-join for a nested tree
        if ((joinType==JoinRelType.DEFAULT || joinType==JoinRelType.INNER) && leftInput.joinTables!=null && rightInput.joinTables!=null
                && !leftInput.hasPullups() && !rightInput.hasPullups() && eqDecomp.getRemainingPredicates().isEmpty()) {
            //Determine if we can map the tables from both branches of the join onto each-other
            int leftTargetLength = leftInput.indexMap.getTargetLength();
            Map<JoinTable, JoinTable> right2left = JoinTable.joinTreeMap(leftInput.joinTables,
                    leftTargetLength , rightInput.joinTables, eqDecomp.getEqualities());
            if (!right2left.isEmpty()) {
                //We currently expect a single path from leaf to right as a self-join
                Preconditions.checkArgument(JoinTable.getRoots(rightInput.joinTables).size() == 1, "Current simplifying assumption");
                JoinTable rightLeaf = Iterables.getOnlyElement(JoinTable.getLeafs(rightInput.joinTables));
                RelBuilder relBuilder = relBuilderFactory.get().push(leftInput.getRelNode());
                ContinuousIndexMap newPk = leftInput.primaryKey;
                List<JoinTable> joinTables = new ArrayList<>(leftInput.joinTables);
                if (!right2left.containsKey(rightLeaf)) {
                    //Find closest ancestor that was mapped and shred from there
                    List<JoinTable> ancestorPath = new ArrayList<>();
                    int numAddedPks = 0;
                    ancestorPath.add(rightLeaf);
                    JoinTable ancestor = rightLeaf;
                    while (!right2left.containsKey(ancestor)) {
                        numAddedPks += ancestor.getNumLocalPk();
                        ancestor = ancestor.parent;
                        ancestorPath.add(ancestor);
                    }
                    Collections.reverse(ancestorPath); //To match the order of addedTables when shredding (i.e. from root to leaf)
                    ContinuousIndexMap.Builder addedPk = ContinuousIndexMap.builder(newPk, numAddedPks);
                    List<JoinTable> addedTables = new ArrayList<>();
                    relBuilder = shredTable(rightLeaf.table, addedPk, addedTables,
                            Pair.of(right2left.get(ancestor),relBuilder));
                    newPk = addedPk.build(relBuilder.peek().getRowType().getFieldCount());
                    Preconditions.checkArgument(ancestorPath.size() == addedTables.size());
                    for (int i = 1; i < addedTables.size(); i++) { //First table is the already mapped root ancestor
                        joinTables.add(addedTables.get(i));
                        right2left.put(ancestorPath.get(i), addedTables.get(i));
                    }
                }
                RelNode relNode = relBuilder.build();
                //Update indexMap based on the mapping of join tables
                final ProcessedRel rightInputfinal = rightInput;
                ContinuousIndexMap remapedRight = rightInput.indexMap.remap(relNode.getRowType().getFieldCount(),
                        index -> {
                            JoinTable jt = JoinTable.find(rightInputfinal.joinTables,index).get();
                            return right2left.get(jt).getGlobalIndex(jt.getLocalIndex(index));
                        });
                ContinuousIndexMap indexMap = leftInput.indexMap.append(remapedRight);
                return setRelHolder(new ProcessedRel(relNode, leftInput.type,
                        newPk, leftInput.timestamp, indexMap, joinTables, NowFilter.EMPTY, TopNConstraint.EMPTY));

            }
        }

        final ProcessedRel leftInputF = leftInput.inlinePullups();
        final ProcessedRel rightInputF = rightInput.inlinePullups();
        RelBuilder relB = relBuilderFactory.get();

        //Detect temporal join
        if (joinType==JoinRelType.DEFAULT || joinType==JoinRelType.TEMPORAL) {
            if ((leftInputF.type==TableType.STREAM && rightInputF.type==TableType.TEMPORAL_STATE) ||
                    (rightInputF.type==TableType.STREAM && leftInputF.type==TableType.TEMPORAL_STATE)) {
                //Check for primary keys equalities on the state-side of the join
                final Function<IntPair,Integer> pkAccess;
                final ContinuousIndexMap pk;
                if (rightInputF.type==TableType.TEMPORAL_STATE) {
                    pkAccess = (p -> p.target);
                    pk = rightInputF.primaryKey;
                } else {
                    pkAccess = (p -> p.source);
                    pk = leftInputF.primaryKey;
                }
                Set<Integer> pkIndexes = pk.getMapping().map(p-> p.getTarget()).collect(Collectors.toSet());
                Set<Integer> pkEqualities = eqDecomp.getEqualities().stream().map(pkAccess).collect(Collectors.toSet());
                if (pkIndexes.equals(pkEqualities) && eqDecomp.getRemainingPredicates().isEmpty()) {
                    joinType = JoinRelType.TEMPORAL;
                    //Construct temporal correlate join
                    return null;
                } else if (joinType==JoinRelType.TEMPORAL) {
                    throw new IllegalArgumentException("Expected join condition to be equality condition on state's primary key: " + logicalJoin);
                }
            } else if (joinType==JoinRelType.TEMPORAL) {
                throw new IllegalArgumentException("Expect one side of the join to be stream and the other temporal state: " + logicalJoin);
            }

        }

        //Detect interval join
        if (joinType==JoinRelType.DEFAULT || joinType ==JoinRelType.INNER /*|| joinType==JoinRelType.INTERVAL*/) {
            if (leftInputF.type==TableType.STREAM && rightInputF.type==TableType.STREAM) {

            }
        }

        //If we don't detect a special time-based join, a DEFAULT join is an INNER join
        if (joinType==JoinRelType.DEFAULT) {
            joinType = JoinRelType.INNER;
        }

        Preconditions.checkArgument(joinType == JoinRelType.INNER, "Unsupported join type: %s", logicalJoin);
        //Default inner join creates a state table
        RelNode newJoin = relB.push(leftInputF.relNode).push(rightInputF.getRelNode())
                .join(JoinRelType.INNER, condition).build();
        ContinuousIndexMap.Builder concatPk = ContinuousIndexMap.builder(leftInputF.primaryKey,rightInputF.primaryKey.getSourceLength());
        concatPk.addAll(rightInputF.primaryKey.remap(joinedIndexMap.getTargetLength(), idx -> idx + leftInputF.indexMap.getTargetLength()));
        return setRelHolder(new ProcessedRel(newJoin, TableType.STATE,
                concatPk.build(joinedIndexMap.getTargetLength()), TimestampHolder.Derived.NONE,
                joinedIndexMap, null, NowFilter.EMPTY, TopNConstraint.EMPTY));
    }

    @Override
    public RelNode visit(LogicalUnion logicalUnion) {
        throw new UnsupportedOperationException("Not yet supported");
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        //Need to inline TopN before we aggregate, but we postpone inlining now-filter in case we can push it through
        final ProcessedRel input = getRelHolder(aggregate.getInput().accept(this)).inlineTopN();
        Preconditions.checkArgument(aggregate.groupSets.size()==1,"Do not yet support GROUPING SETS.");
        List<Integer> groupByIdx = aggregate.getGroupSet().asList().stream()
                .map(idx -> input.indexMap.map(idx))
                .collect(Collectors.toList());
        List<AggregateCall> aggregateCalls = aggregate.getAggCallList().stream().map(agg -> {
            Preconditions.checkArgument(agg.getCollation().getFieldCollations().isEmpty(), "Unexpected aggregate call: %s", agg);
            Preconditions.checkArgument(agg.filterArg<0,"Unexpected aggregate call: %s", agg);
            return agg.copy(agg.getArgList().stream().map(idx -> input.indexMap.map(idx)).collect(Collectors.toList()));
        }).collect(Collectors.toList());
        int targetLength = groupByIdx.size() + aggregateCalls.size();


        //Check if this is a time-window aggregation
        if (input.type == TableType.STREAM && input.getRelNode() instanceof LogicalProject) {
            //Determine if one of the groupBy keys is a timestamp
            TimestampHolder.Candidate keyCandidate = null;
            int keyIdx = -1, inputColIdx = -1;
            for (int i = 0; i < groupByIdx.size(); i++) {
                int idx = groupByIdx.get(i);
                Optional<TimestampHolder.Candidate> candidate = input.timestamp.getCandidateByIndex(idx);
                if (candidate.isPresent()) {
                    Preconditions.checkArgument(keyCandidate==null, "Do not currently support aggregating by multiple timestamp columns");
                    keyCandidate = candidate.get();
                    keyIdx = i;
                    inputColIdx = idx;
                }
            }
            if (keyCandidate!=null) {
                LogicalProject inputProject = (LogicalProject)input.getRelNode();
                Optional<TimeBucketFunctionCall> bucketFct = rexUtil.getTimeBucketingFunction(inputProject.getProjects().get(inputColIdx));
                Preconditions.checkArgument(!bucketFct.isEmpty());

                //Fix timestamp (if not already fixed)
                TimestampHolder.Derived newTimestamp = input.timestamp.fixTimestamp(keyCandidate.getIndex(), keyIdx);

                //TODO: support moving now-filters through since those are guaranteed to be on the same timestamp
                //and can be converted to filters on the time bucket post-aggregation,
                // i.e. time_bucket_col > time_bucket_function(now()) - INTERVAL X;  for now-filter: time_col > now() - INTERVAL X
                Preconditions.checkArgument(input.nowFilter.isEmpty(), "Now-filters in combination with timestamp aggregation aren't yet supported");

                RelBuilder relB = relBuilderFactory.get();
                relB.push(input.relNode);
                relB.aggregate(relB.groupKey(ImmutableBitSet.of(groupByIdx)),aggregateCalls);
                relB.hints(new TimeAggregationHint(TimeAggregationHint.Type.TUMBLE).getHint());
                ContinuousIndexMap pk = ContinuousIndexMap.identity(groupByIdx.size(), targetLength);
                ContinuousIndexMap indexMap = ContinuousIndexMap.identity(targetLength, targetLength);

                return setRelHolder(new ProcessedRel(relB.build(), TableType.STREAM, pk,
                        newTimestamp, indexMap,null, NowFilter.EMPTY, TopNConstraint.EMPTY));
                //TODO: this type of streaming aggregation requires a post-filter in the database (in physical model) to filter out "open" time buckets,
                //i.e. time_bucket_col < time_bucket_function(now()) [if now() lands in a time bucket, that bucket is still open and shouldn't be shown]
            }
        }

        //Inline now-filter at this point
        ProcessedRel nowInput = input;
        boolean isSlidingAggregate = false;
        if (!input.nowFilter.isEmpty()) {
            nowInput = input.inlineNowFilter();
            isSlidingAggregate = true;
            //TODO: extract slide-width from hint
        }

        //Check if we need to propagate timestamps
        if (nowInput.type == TableType.STREAM || nowInput.type == TableType.TEMPORAL_STATE) {
            //TODO: Convert to window-over in order to preserve timestamp
            targetLength += 1;

            //Fix best timestamp (if not already fixed) and add as final project
            TimestampHolder.Derived inputTimestamp = nowInput.timestamp;
            TimestampHolder.Candidate candidate = inputTimestamp.getBestCandidate();
            TimestampHolder.Derived addedTimestamp = inputTimestamp.fixTimestamp(candidate.getIndex(),targetLength-1);

            RelNode inputRel = nowInput.relNode;
            RelBuilder relB = relBuilderFactory.get();
            relB.push(inputRel);

            RexInputRef timestampRef = RexInputRef.of(candidate.getIndex(), inputRel.getRowType());

            List<RexNode> partitionKeys = new ArrayList<>(groupByIdx.size());
            List<RexNode> projects = new ArrayList<>(targetLength);
            List<String> projectNames = new ArrayList<>(targetLength);
            //Add groupByKeys
            for (Integer keyIdx : groupByIdx) {
                RexInputRef ref = RexInputRef.of(keyIdx, inputRel.getRowType());
                projects.add(ref);
                projectNames.add(null);
                partitionKeys.add(ref);
            }
            RexFieldCollation orderBy = new RexFieldCollation(timestampRef, Set.of(SqlKind.DESCENDING));

            //Add aggregate functions
            for (int i = 0; i < aggregateCalls.size(); i++) {
                AggregateCall call = aggregateCalls.get(i);
                RexNode agg = relB.getRexBuilder().makeOver(call.getType(),call.getAggregation(),
                        call.getArgList().stream()
                                .map(idx -> RexInputRef.of(idx,inputRel.getRowType()))
                                .collect(Collectors.toList()),
                        partitionKeys,
                        ImmutableList.of(orderBy),
                        RexWindowBounds.UNBOUNDED_PRECEDING,
                        RexWindowBounds.CURRENT_ROW,
                        true, true, false, false, true
                        );
                projects.add(agg);
                projectNames.add(aggregate.getNamedAggCalls().get(i).getValue());
            }
            projects.add(timestampRef);
            projectNames.add(null);

            relB.project(projects,projectNames);
            if (isSlidingAggregate) relB.hints(new TimeAggregationHint(TimeAggregationHint.Type.SLIDING).getHint());
            ContinuousIndexMap pk = ContinuousIndexMap.identity(groupByIdx.size(), targetLength);
            ContinuousIndexMap indexMap = ContinuousIndexMap.identity(targetLength-1, targetLength);
            return setRelHolder(new ProcessedRel(relB.build(), TableType.TEMPORAL_STATE, pk,
                    addedTimestamp, indexMap,null, NowFilter.EMPTY, TopNConstraint.EMPTY));
        }

        //Standard aggregation produces a state table
        RelBuilder relB = relBuilderFactory.get();
        relB.push(nowInput.relNode);
        relB.aggregate(relB.groupKey(ImmutableBitSet.of(groupByIdx)),aggregateCalls);
        if (isSlidingAggregate) relB.hints(new TimeAggregationHint(TimeAggregationHint.Type.SLIDING).getHint());
        ContinuousIndexMap pk = ContinuousIndexMap.identity(groupByIdx.size(), targetLength);
        ContinuousIndexMap indexMap = ContinuousIndexMap.identity(targetLength, targetLength);
        return setRelHolder(new ProcessedRel(relB.build(), TableType.STATE, pk,
                TimestampHolder.Derived.NONE, indexMap,null, NowFilter.EMPTY, TopNConstraint.EMPTY));
    }

    @Override
    public RelNode visit(LogicalSort logicalSort) {
        Preconditions.checkArgument(logicalSort.offset == null, "OFFSET not yet supported");
        ProcessedRel input = getRelHolder(logicalSort.getInput().accept(this));
        Preconditions.checkArgument(!input.topN.hasPartition(),"Sorting on top of a partitioned relation is invalid");
        if (input.topN.isDistinct() || input.topN.hasLimit()) {
            //Need to inline before we can sort on top
            input = input.inlineTopN();
        } //else there is only a sort which we replace by this sort if present

        RelCollation collation = logicalSort.getCollation();
        //Map the collation fields
        ContinuousIndexMap indexMap = input.indexMap;
        RelCollation newCollation = RelCollations.of(collation.getFieldCollations().stream()
                .map(fc -> fc.withFieldIndex(indexMap.map(fc.getFieldIndex()))).collect(Collectors.toList()));
        if (newCollation.getFieldCollations().isEmpty()) newCollation = input.topN.getCollation();

        TopNConstraint topN = new TopNConstraint(newCollation, List.of(), getLimit(logicalSort.fetch), false);

        return setRelHolder(new ProcessedRel(input.relNode,input.type,input.primaryKey,
                input.timestamp,input.indexMap,input.joinTables, input.nowFilter, topN));
    }

    public Optional<Integer> getLimit(RexNode limit) {
        Preconditions.checkArgument(limit instanceof RexLiteral);
        return Optional.of(((RexLiteral)limit).getValueAs(Integer.class));
    }




}
