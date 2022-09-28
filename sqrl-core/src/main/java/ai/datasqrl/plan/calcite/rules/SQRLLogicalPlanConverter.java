package ai.datasqrl.plan.calcite.rules;

import ai.datasqrl.function.SqrlAwareFunction;
import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.calcite.hints.TemporalJoinHint;
import ai.datasqrl.plan.calcite.hints.TimeAggregationHint;
import ai.datasqrl.plan.calcite.table.*;
import ai.datasqrl.plan.calcite.util.*;
import ai.datasqrl.plan.global.MaterializationPreference;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Value
public class SQRLLogicalPlanConverter extends AbstractSqrlRelShuttle<SQRLLogicalPlanConverter.RelMeta> {

    private static final long UPPER_BOUND_INTERVAL_MS = 999l*365l*24l*3600l; //999 years
    public static final double HIGH_CARDINALITY_JOIN_THRESHOLD = 10000;

    public final Supplier<RelBuilder> relBuilderFactory;
    public final SqrlRexUtil rexUtil;
    public final MaterializationPreference defaultMaterialization = MaterializationPreference.SHOULD;
    public final double cardinalityJoinThreshold = HIGH_CARDINALITY_JOIN_THRESHOLD;

    public SQRLLogicalPlanConverter(Supplier<RelBuilder> relBuilderFactory) {
        this.relBuilderFactory = relBuilderFactory;
        this.rexUtil = new SqrlRexUtil(relBuilderFactory.get().getTypeFactory());
    }

    @Value
    @AllArgsConstructor
    public class RelMeta implements RelHolder {

        RelNode relNode;
        TableType type;
        ContinuousIndexMap primaryKey;
        TimestampHolder.Derived timestamp;
        ContinuousIndexMap indexMap;

        List<JoinTable> joinTables;
        MaterializationInference materialize;

        NowFilter nowFilter; //Applies before dedup
        Deduplication dedup;

        TableStatistic statistic;

        public RelMeta(RelNode relNode, TableType type, ContinuousIndexMap primaryKey,
                       TimestampHolder.Derived timestamp, ContinuousIndexMap indexMap,
                       List<JoinTable> joinTables, MaterializationInference materialize,
                       NowFilter nowFilter, Deduplication dedup) {
            this(relNode,type,primaryKey,timestamp,indexMap,joinTables,materialize,nowFilter,dedup,null);
        }

        /**
         * Called to inline the TopNConstraint on top of the input relation
         * @return
         */
        public RelMeta inlineDedup() {
            if (dedup.isEmpty()) return this;
            return inlinePullups(); //Inlining dedup requires inlining nowFilter first
        }

        public RelMeta inlineNowFilter() {
            if (nowFilter.isEmpty()) return this;
            RelBuilder relB = relBuilderFactory.get();
            relB.push(relNode);
            nowFilter.addFilterTo(relB);
            return new RelMeta(relB.build(),type,primaryKey,timestamp,indexMap,joinTables,
                    materialize.update(MaterializationPreference.MUST, "now-filter"), NowFilter.EMPTY, dedup);
        }

        public boolean hasPullups() {
            return !dedup.isEmpty() || !nowFilter.isEmpty();
        }

        public PullupOperator.Container getPullups() {
            return new PullupOperator.Container(dedup,nowFilter);
        }

        public RelMeta inlinePullups() {
            RelMeta result = this;
            if (!nowFilter.isEmpty()) {
                result = inlineNowFilter();
            }
            Preconditions.checkArgument(dedup.isEmpty(), "not yet supported");
            return result;
        }
    }

    /**
     * Moves the primary key columns to the front and adds projection to only return
     * columns that the user selected, are part of the primary key, or a timestamp candidate.
     *
     * Inlines deduplication in case of nested data.
     *
     * @param input
     * @return
     */
    public RelMeta postProcess(RelMeta input, List<String> fieldNames,
                               Optional<MaterializationPreference> materializationPreference) {
        Preconditions.checkArgument(fieldNames.size()==input.indexMap.getSourceLength());
        if (CalciteUtil.hasNesting(input.getRelNode().getRowType()) && !input.getDedup().isEmpty()) {
            //Need to inline deduplication for nested tables
            input = input.inlineDedup();
        }
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
        IndexMap remap = IndexMap.of(remapping);
        ContinuousIndexMap updatedIndexMap = input.indexMap.remap(remap);
        List<RexNode> projects = new ArrayList<>(indexMap.getTargetLength());
        RelDataType rowType = input.relNode.getRowType();
        remapping.entrySet().stream().map(e -> new IndexMap.Pair(e.getKey(),e.getValue()))
                .sorted((a, b)-> Integer.compare(a.getTarget(),b.getTarget()))
                .forEach(p -> {
            projects.add(p.getTarget(),RexInputRef.of(p.getSource(), rowType));
        });
        List<String> updatedFieldNames = Arrays.asList(new String[projects.size()]);
        for (int i = 0; i < fieldNames.size(); i++) {
            updatedFieldNames.set(updatedIndexMap.map(i),fieldNames.get(i));
        }
        RelBuilder relBuilder = relBuilderFactory.get();
        relBuilder.push(input.relNode);
        relBuilder.project(projects, updatedFieldNames);
        RelNode relNode = relBuilder.build();
        TableStatistic statistic = TableStatistic.of(estimateRowCount(relNode));

        MaterializationInference materialize = input.materialize;
        if (materializationPreference.isPresent()) {
            materialize = materialize.update(materializationPreference.get(),"user defined");
        }

        return new RelMeta(relNode,input.type,input.primaryKey.remap(remap),
                input.timestamp.remapIndexes(remap), updatedIndexMap, null,
                materialize, input.nowFilter.remap(remap), input.dedup.remap(remap), statistic);
    }

    private double estimateRowCount(RelNode node) {
        final RelMetadataQuery mq = node.getCluster().getMetadataQuery();
        return mq.getRowCount(node);
    }


    private RelMeta extractTopNConstraint(RelMeta input, LogicalProject project) {
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
        AtomicReference<MaterializationInference> materialize = new AtomicReference<>(new MaterializationInference(defaultMaterialization));
        //Now, we shred
        RelNode relNode = shredTable(vtable, primaryKey, indexMap, joinTables, materialize, true).build();
        //Finally, we assemble the result
        VirtualRelationalTable.Root root = vtable.getRoot();
        QueryRelationalTable queryTable = root.getBase();
        int mapToLength = relNode.getRowType().getFieldCount();
        RelMeta result = new RelMeta(relNode, queryTable.getType(),
                primaryKey.build(mapToLength),
                new TimestampHolder.Derived(queryTable.getTimestamp()),
                indexMap.build(mapToLength), joinTables, materialize.get(),
                queryTable.getPullups().getNowFilter(), queryTable.getPullups().getDeduplication());
        return setRelHolder(result);
    }

    private RelBuilder shredTable(VirtualRelationalTable vtable, ContinuousIndexMap.Builder primaryKey,
                                  ContinuousIndexMap.Builder indexMap, List<JoinTable> joinTables,
                                  AtomicReference<MaterializationInference> materialize,
                                  boolean isLeaf) {
        Preconditions.checkArgument(joinTables.isEmpty());
        return shredTable(vtable, primaryKey, indexMap, joinTables, materialize,null, isLeaf);
    }

    private RelBuilder shredTable(VirtualRelationalTable vtable, ContinuousIndexMap.Builder primaryKey,
                                  List<JoinTable> joinTables, Pair<JoinTable,RelBuilder> startingBase,
                                  AtomicReference<MaterializationInference> materialize) {
        Preconditions.checkArgument(joinTables.isEmpty());
        return shredTable(vtable, primaryKey, null, joinTables, materialize, startingBase, false);
    }

    private RelBuilder shredTable(VirtualRelationalTable vtable, ContinuousIndexMap.Builder primaryKey,
                                  ContinuousIndexMap.Builder indexMap, List<JoinTable> joinTables,
                                  AtomicReference<MaterializationInference> materialize,
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
        } else {
            VirtualRelationalTable.Child child = (VirtualRelationalTable.Child) vtable;
            builder = shredTable(child.getParent(), primaryKey, indexMap, joinTables, materialize, startingBase,false);
            JoinTable parentJoinTable = Iterables.getLast(joinTables);
            int indexOfShredField = parentJoinTable.getOffset() + child.getShredIndex();
            CorrelationId id = new CorrelationId(0);
            RelDataType base = builder.peek().getRowType();
            offset = base.getFieldCount();

            builder
                    .values(List.of(List.of(rexUtil.getBuilder().makeExactLiteral(BigDecimal.ZERO))),
                            new RelRecordType(List.of(new RelDataTypeFieldImpl(
                                    "ZERO",
                                    0,
                                    builder.getTypeFactory().createSqlType(SqlTypeName.INTEGER)))))
                    .project(
                            List.of(rexUtil.getBuilder()
                                    .makeFieldAccess(
                                            rexUtil.getBuilder().makeCorrel(base, id),
                                            indexOfShredField)))
                    .uncollect(List.of(), false)
                    .correlate(JoinRelType.INNER, id, RexInputRef.of(indexOfShredField,  base));
            joinTable = new JoinTable(vtable, parentJoinTable, offset);
            materialize.getAndUpdate(m -> m.update(MaterializationPreference.MUST,"unnesting"));
        }
        for (int i = 0; i < vtable.getNumLocalPks(); i++) {
            primaryKey.add(offset+i);
            if (!isLeaf && startingBase==null) indexMap.add(offset+i);
        }
        //Add additional columns
        JoinTable.Path path = JoinTable.Path.of(joinTable);
        for (AddedColumn column : vtable.getAddedColumns()) {
            //How do columns impact materialization preference (e.g. contain function that cannot be computed in DB) if they might get projected out again
            List<RexNode> projects = rexUtil.getIdentityProject(builder.peek());
            RexNode added;
            if (column instanceof AddedColumn.Simple) {
                added = ((AddedColumn.Simple) column).getExpression(path);
            } else {
                AddedColumn.Complex cc = (AddedColumn.Complex) column;
                //TODO: Need to join and project out everything but the last column
                throw new UnsupportedOperationException("Not yet implemented");
            }
            projects.add(added);
            builder.project(projects);
        }
        joinTables.add(joinTable);
        //Construct indexMap if this shred table is the leaf (i.e. the one we are expanding)
        if (isLeaf && startingBase==null) {
            //All non-nested fields are part of the virtual table query row type
            List<RelDataTypeField> queryRowType = vtable.getQueryRowType().getFieldList();
            for (int i = 0; i < queryRowType.size(); i++) {
                RelDataTypeField field = queryRowType.get(i);
                if (!CalciteUtil.isNestedTable(field.getType())) {
                    indexMap.add(offset+i);
                }
            }
        }
        return builder;
    }

    private static final SqrlRexUtil.RexFinder FIND_NOW = SqrlRexUtil.findFunction(SqrlOperatorTable.NOW);
    //Functions that can only be executed in the database
    private static final SqrlRexUtil.RexFinder FIND_DB_ONLY = SqrlRexUtil.findFunction(SqrlOperatorTable.NOW);
    //Functions that can only be executed in the stream
    private static final SqrlRexUtil.RexFinder FIND_STREAM_ONLY = SqrlRexUtil.findFunction(o -> {
        return (o instanceof SqrlAwareFunction) && !o.equals(SqrlOperatorTable.NOW);
    });
    private static final Predicate<SqlAggFunction> STREAM_ONLY_AGG = Predicates.alwaysFalse();

    @Override
    public RelNode visit(LogicalFilter logicalFilter) {
        RelMeta input = getRelHolder(logicalFilter.getInput().accept(this));
        input = input.inlineDedup(); //Filtering doesn't preserve deduplication
        RexNode condition = logicalFilter.getCondition();
        condition = SqrlRexUtil.mapIndexes(condition,input.indexMap);
        TimestampHolder.Derived timestamp = input.timestamp;
        NowFilter nowFilter = input.nowFilter;

        //Check if it has a now() predicate and pull out or throw an exception if malformed
        RelBuilder relBuilder = relBuilderFactory.get();
        relBuilder.push(input.relNode);
        List<TimePredicate> timeFunctions = new ArrayList<>();
        List<RexNode> conjunctions = null;
        if (FIND_NOW.foundIn(condition)) {
            conjunctions = rexUtil.getConjunctions(condition);
            Iterator<RexNode> iter = conjunctions.iterator();
            while (iter.hasNext()) {
                RexNode conj = iter.next();
                if (FIND_NOW.foundIn(conj)) {
                    Optional<TimePredicate> tp = TimePredicate.ANALYZER.extractTimePredicate(conj, rexUtil.getBuilder(),
                                    timestamp.isCandidatePredicate())
                            .filter(TimePredicate::hasTimestampFunction);
                    if (tp.isPresent() && tp.get().isNowPredicate()) {
                        timeFunctions.add(tp.get());
                        iter.remove();
                    } else {
                        /*Filter is not on a timestamp or not parsable, we leave it as is. In the future we can consider
                        pulling up now-filters on non-timestamp columns since we need to push those into the database
                        anyways. However, we cannot do much else with those (e.g. convert to time-windows or TTL) since
                        they are not based on the timeline.
                         */
                        //TODO: issue warning
                    }
                }
            }
        }
        if (!timeFunctions.isEmpty()) {
            Optional<NowFilter> resultFilter = nowFilter.addAll(timeFunctions);
            if (resultFilter.isEmpty()) throw new IllegalArgumentException("Combined now-filters have an empty result-set");
            nowFilter = resultFilter.get();
            int timestampIdx = nowFilter.getTimestampIndex();
            timestamp = timestamp.fixTimestamp(timestampIdx);
        } else {
            conjunctions = List.of(condition);
        }
        relBuilder.filter(conjunctions);
        return setRelHolder(new RelMeta(relBuilder.build(),input.type,input.primaryKey,
                timestamp,input.indexMap,input.joinTables, analyzeRexNodesForMaterialize(input.materialize, conjunctions),
                nowFilter, input.dedup));
    }

    private MaterializationInference analyzeRexNodesForMaterialize(MaterializationInference base, Iterable<RexNode> nodes) {
        //Update materialization preference based on function calls in RexNodes
        MaterializationInference result = base;
        if (FIND_DB_ONLY.foundIn(nodes)) result = result.update(MaterializationPreference.CANNOT,"DB-only function call");
        if (FIND_STREAM_ONLY.foundIn(nodes)) result = result.update(MaterializationPreference.MUST,"stream-only function call");
        return result;
    }

    @Override
    public RelNode visit(LogicalProject logicalProject) {
        RelMeta rawInput = getRelHolder(logicalProject.getInput().accept(this));
        rawInput = extractTopNConstraint(rawInput, logicalProject);


        ContinuousIndexMap trivialMap = getTrivialMapping(logicalProject, rawInput.indexMap);
        if (trivialMap!=null) {
            //If it's a trivial project, we remove it and only update the indexMap. This is needed to eliminate self-joins
            return setRelHolder(new RelMeta(rawInput.relNode,rawInput.type,rawInput.primaryKey, rawInput.timestamp,
                    trivialMap, rawInput.joinTables, rawInput.materialize, rawInput.nowFilter, rawInput.dedup));
        }
        RelMeta input = rawInput.inlineDedup();
        Preconditions.checkArgument(input.dedup.isEmpty());
        //Update index mappings
        List<RexNode> updatedProjects = new ArrayList<>();
        List<String> updatedNames = new ArrayList<>();
        //We only keep track of the first mapped project and consider it to be the "preserving one" for primary keys and timestamps
        Map<Integer,Integer> mappedProjects = new HashMap<>();
        List<TimestampHolder.Candidate> timeCandidates = new ArrayList<>();
        NowFilter nowFilter = NowFilter.EMPTY;
        for (Ord<RexNode> exp : Ord.<RexNode>zip(logicalProject.getProjects())) {
            RexNode mapRex = SqrlRexUtil.mapIndexes(exp.e,input.indexMap);
            updatedProjects.add(exp.i,mapRex);
            updatedNames.add(exp.i,logicalProject.getRowType().getFieldNames().get(exp.i));
            int originalIndex = -1;
            if (mapRex instanceof RexInputRef) { //Direct mapping
                originalIndex = (((RexInputRef) mapRex)).getIndex();
            } else { //Check for preserved timestamps
                Optional<TimestampHolder.Candidate> preservedCandidate = rexUtil.getPreservedTimestamp(mapRex, input.timestamp);
                if (preservedCandidate.isPresent()) {
                    originalIndex = preservedCandidate.get().getIndex();
                    timeCandidates.add(preservedCandidate.get().withIndex(exp.i));
                    //See if we can preserve the now-filter as well or need to inline it
                    if (!input.nowFilter.isEmpty() && input.nowFilter.getTimestampIndex()==originalIndex) {
                        Optional<TimeTumbleFunctionCall> bucketFct = rexUtil.getTimeBucketingFunction(mapRex);
                        if (bucketFct.isPresent()) {
                            long intervalExpansion = bucketFct.get().getSpecification().getBucketWidthMillis();
                            nowFilter = input.nowFilter.map(tp -> new TimePredicate(tp.getSmallerIndex(),
                                    exp.i,tp.isSmaller(),tp.getInterval_ms()+intervalExpansion));
                        } else {
                            input = input.inlineNowFilter();
                        }
                    }
                }
            }
            if (originalIndex>0) {
                if (mappedProjects.putIfAbsent(originalIndex,exp.i)!=null) {
                    //We are ignoring this mapping because the prior one takes precedence, let's see if we should warn the user
                    if (input.primaryKey.containsTarget(originalIndex) || input.timestamp.isCandidate(originalIndex)) {
                        //TODO: convert to warning and ignore
                        throw new IllegalArgumentException("Cannot project primary key or timestamp columns multiple times");
                    }
                }
            }
        }
        //Make sure we pull the primary keys and timestamp candidates through (i.e. append those to the projects
        //if not already present)
        ContinuousIndexMap.Builder primaryKey = ContinuousIndexMap.builder(input.primaryKey.getSourceLength());
        for (IndexMap.Pair p: input.primaryKey.getMapping()) {
            Integer target = mappedProjects.get(p.getTarget());
            if (target!=null) primaryKey.add(target);
            else {
                //Need to add it
                int index = updatedProjects.size();
                updatedProjects.add(index,RexInputRef.of(p.getTarget(),input.relNode.getRowType()));
                updatedNames.add(null);
                primaryKey.add(index);
            }
        }
        for (TimestampHolder.Candidate candidate : input.timestamp.getCandidates()) {
            //Check if candidate is already mapped through timestamp preserving function
            if (timeCandidates.stream().anyMatch(c -> c.getId() == candidate.getId())) continue;
            Integer target = mappedProjects.get(candidate.getIndex());
            if (target==null) {
                //Need to add candidate
                int index = updatedProjects.size();
                updatedProjects.add(index,RexInputRef.of(candidate.getIndex(),input.relNode.getRowType()));
                updatedNames.add(null);
                timeCandidates.add(candidate.withIndex(index));
            } else {
                timeCandidates.add(candidate.withIndex(target));
                //Update now-filter if it matches candidate
                if (!input.nowFilter.isEmpty() && input.nowFilter.getTimestampIndex()== candidate.getIndex()) {
                    nowFilter = input.nowFilter.remap(IndexMap.singleton(candidate.getIndex(), target));
                }
            }
        }
        TimestampHolder.Derived timestamp = input.timestamp.propagate(timeCandidates);
        //NowFilter must have been preserved
        assert !nowFilter.isEmpty() || input.nowFilter.isEmpty();

        //Build new project
        RelBuilder relB = relBuilderFactory.get();
        relB.push(input.relNode);
        relB.project(updatedProjects,updatedNames);
        RelNode newProject = relB.build();
        int fieldCount = updatedProjects.size();
        return setRelHolder(new RelMeta(newProject,input.type,primaryKey.build(fieldCount),
                timestamp, ContinuousIndexMap.identity(logicalProject.getProjects().size(),fieldCount),null,
                analyzeRexNodesForMaterialize(input.materialize,updatedProjects),
                nowFilter, Deduplication.EMPTY));
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
        RelMeta leftInput = getRelHolder(logicalJoin.getLeft().accept(this));
        RelMeta rightInput = getRelHolder(logicalJoin.getRight().accept(this));
        JoinRelType joinType = logicalJoin.getJoinType();


        ContinuousIndexMap joinedIndexMap = leftInput.indexMap.join(rightInput.indexMap);
        RexNode condition = SqrlRexUtil.mapIndexes(logicalJoin.getCondition(),joinedIndexMap);
        //TODO: pull now() conditions up as a nowFilter and move nested now filters through
        Preconditions.checkArgument(!FIND_NOW.foundIn(condition),"now() is not allowed in join conditions");
        SqrlRexUtil.EqualityComparisonDecomposition eqDecomp = rexUtil.decomposeEqualityComparison(condition);

        int leftSideMaxIdx = leftInput.indexMap.getTargetLength();

        MaterializationInference combinedMaterialize = leftInput.materialize.combine(rightInput.materialize);
        combinedMaterialize = analyzeRexNodesForMaterialize(combinedMaterialize,List.of(condition));

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
                AtomicReference<MaterializationInference> materialize = new AtomicReference<>(combinedMaterialize);
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
                            Pair.of(right2left.get(ancestor),relBuilder),materialize);
                    newPk = addedPk.build(relBuilder.peek().getRowType().getFieldCount());
                    Preconditions.checkArgument(ancestorPath.size() == addedTables.size());
                    for (int i = 1; i < addedTables.size(); i++) { //First table is the already mapped root ancestor
                        joinTables.add(addedTables.get(i));
                        right2left.put(ancestorPath.get(i), addedTables.get(i));
                    }
                }
                RelNode relNode = relBuilder.build();
                //Update indexMap based on the mapping of join tables
                final RelMeta rightInputfinal = rightInput;
                ContinuousIndexMap remapedRight = rightInput.indexMap.remap(relNode.getRowType().getFieldCount(),
                        index -> {
                            JoinTable jt = JoinTable.find(rightInputfinal.joinTables,index).get();
                            return right2left.get(jt).getGlobalIndex(jt.getLocalIndex(index));
                        });
                ContinuousIndexMap indexMap = leftInput.indexMap.append(remapedRight);
                return setRelHolder(new RelMeta(relNode, leftInput.type, newPk, leftInput.timestamp,
                        indexMap, joinTables, materialize.get(), NowFilter.EMPTY, Deduplication.EMPTY));

            }
        }

        //TODO: pull now-filters through if possible
        leftInput = leftInput.inlinePullups();
        rightInput = rightInput.inlinePullups();

        //Detect temporal join
        if (joinType==JoinRelType.DEFAULT || joinType==JoinRelType.TEMPORAL) {
            if ((leftInput.type==TableType.STREAM && rightInput.type==TableType.TEMPORAL_STATE) ||
                    (rightInput.type==TableType.STREAM && leftInput.type==TableType.TEMPORAL_STATE)) {
                //Make sure the stream is left and state is right
                if (rightInput.type==TableType.STREAM) {
                    //Switch sides
                    RelMeta tmp = rightInput;
                    rightInput = leftInput;
                    leftInput = tmp;
                    joinedIndexMap = leftInput.indexMap.join(rightInput.indexMap);
                    int tmpLeftSideMaxIdx = leftInput.indexMap.getTargetLength();
                    condition = SqrlRexUtil.mapIndexes(logicalJoin.getCondition(),
                            idx -> idx<leftSideMaxIdx?tmpLeftSideMaxIdx+idx:idx-leftSideMaxIdx);
                    eqDecomp = rexUtil.decomposeEqualityComparison(condition);
                }
                int newLeftSideMaxIdx = leftInput.indexMap.getTargetLength();
                //Check for primary keys equalities on the state-side of the join
                Set<Integer> pkIndexes = rightInput.primaryKey.getMapping().stream().map(p-> p.getTarget()+newLeftSideMaxIdx).collect(Collectors.toSet());
                Set<Integer> pkEqualities = eqDecomp.getEqualities().stream().map(p -> p.target).collect(Collectors.toSet());
                if (pkIndexes.equals(pkEqualities) && eqDecomp.getRemainingPredicates().isEmpty()) {
                    joinType = JoinRelType.TEMPORAL;
                    RelBuilder relB = relBuilderFactory.get();
                    relB.push(leftInput.relNode); relB.push(rightInput.relNode);
                    Preconditions.checkArgument(rightInput.timestamp.hasTimestamp());
                    TimestampHolder.Candidate candidate = leftInput.timestamp.getBestCandidate();
                    TimestampHolder.Derived joinTimestamp = leftInput.timestamp.fixTimestamp(candidate.getIndex());

                    ContinuousIndexMap pk = ContinuousIndexMap.builder(leftInput.primaryKey,0)
                            .build(joinedIndexMap.getTargetLength());
                    TemporalJoinHint hint = new TemporalJoinHint(joinTimestamp.getTimestampIndex(),
                            rightInput.timestamp.getTimestampIndex()+newLeftSideMaxIdx,
                            pk.asArray());
                    relB.join(JoinRelType.INNER, condition);
                    hint.addTo(relB);
                    return setRelHolder(new RelMeta(relB.build(), TableType.STREAM,
                            pk, joinTimestamp, joinedIndexMap, null,
                            combinedMaterialize.update(MaterializationPreference.MUST,"temporal join"),
                            NowFilter.EMPTY, Deduplication.EMPTY));
                } else if (joinType==JoinRelType.TEMPORAL) {
                    throw new IllegalArgumentException("Expected join condition to be equality condition on state's primary key: " + logicalJoin);
                }
            } else if (joinType==JoinRelType.TEMPORAL) {
                throw new IllegalArgumentException("Expect one side of the join to be stream and the other temporal state: " + logicalJoin);
            }

        }

        final RelMeta leftInputF = leftInput;
        final RelMeta rightInputF = rightInput;
        RelBuilder relB = relBuilderFactory.get();
        relB.push(leftInputF.relNode); relB.push(rightInputF.relNode);

        ContinuousIndexMap.Builder concatPkBuilder = ContinuousIndexMap.builder(leftInputF.primaryKey,rightInputF.primaryKey.getSourceLength());
        concatPkBuilder.addAll(rightInputF.primaryKey.remap(joinedIndexMap.getTargetLength(), idx -> idx + leftInputF.indexMap.getTargetLength()));
        ContinuousIndexMap concatPk = concatPkBuilder.build(joinedIndexMap.getTargetLength());

        //Detect interval join
        if (joinType==JoinRelType.DEFAULT || joinType ==JoinRelType.INNER || joinType==JoinRelType.INTERVAL) {
            if (leftInputF.type==TableType.STREAM && rightInputF.type==TableType.STREAM) {
                //Validate that the join condition includes time bounds on both sides
                List<RexNode> conjunctions = rexUtil.getConjunctions(condition);
                Predicate<Integer> isTimestampColumn = idx -> idx<leftSideMaxIdx?leftInputF.timestamp.isCandidate(idx):
                                                                                rightInputF.timestamp.isCandidate(idx-leftSideMaxIdx);
                List<TimePredicate> timePredicates = conjunctions.stream().map(rex ->
                                TimePredicate.ANALYZER.extractTimePredicate(rex, rexUtil.getBuilder(),isTimestampColumn))
                        .flatMap(tp -> tp.stream()).filter(tp -> !tp.hasTimestampFunction())
                        //making sure predicate contains columns from both sides of the join
                        .filter(tp -> (tp.getSmallerIndex() < leftSideMaxIdx) ^ (tp.getLargerIndex() < leftSideMaxIdx))
                        .collect(Collectors.toList());
                if (!timePredicates.isEmpty()) {
                    Set<Integer> timestampIndexes = timePredicates.stream().flatMap(tp -> tp.getIndexes().stream()).collect(Collectors.toSet());
                    Preconditions.checkArgument(timestampIndexes.size() == 2, "Invalid interval condition - more than 2 timestamp columns: %s", condition);
                    Preconditions.checkArgument(timePredicates.stream().filter(TimePredicate::isUpperBound).count() == 1,
                            "Expected exactly one upper bound time predicate, but got: %s", condition);
                    int upperBoundTimestampIndex = timePredicates.stream().filter(TimePredicate::isUpperBound)
                            .findFirst().get().getLargerIndex();
                    TimestampHolder.Derived joinTimestamp = null;
                    //Lock in timestamp candidates for both sides and propagate timestamp
                    for (int tidx : timestampIndexes) {
                        TimestampHolder.Derived newTimestamp = apply2JoinSide(tidx, leftSideMaxIdx, leftInputF, rightInputF,
                                (prel, idx) -> prel.timestamp.fixTimestamp(idx, tidx));
                        if (tidx == upperBoundTimestampIndex) joinTimestamp = newTimestamp;
                    }
                    assert joinTimestamp != null;

                    if (timePredicates.size() == 1 && !timePredicates.get(0).isEquality()) {
                        //We only have an upper bound, add (very loose) bound in other direction - Flink requires this
                        conjunctions = new ArrayList<>(conjunctions);
                        final RexNode findInCondition = condition;
                        conjunctions.add(Iterables.getOnlyElement(timePredicates)
                                .inverseWithInterval(UPPER_BOUND_INTERVAL_MS).createRexNode(rexUtil.getBuilder(),
                                        idx -> SqrlRexUtil.findRexInputRefByIndex(idx).find(findInCondition).get()));
                        condition = RexUtil.composeConjunction(rexUtil.getBuilder(), conjunctions);
                    }
                    joinType = JoinRelType.INTERVAL;
                    relB.join(JoinRelType.INNER, condition); //Can treat as "standard" inner join since no modification is necessary in physical plan
                    return setRelHolder(new RelMeta(relB.build(), TableType.STREAM,
                            concatPk, joinTimestamp, joinedIndexMap,
                            null, combinedMaterialize, NowFilter.EMPTY, Deduplication.EMPTY));
                } else if (joinType==JoinRelType.INTERVAL) {
                    throw new IllegalArgumentException("Interval joins require time bounds in the join condition: " + logicalJoin);
                }
            } else if (joinType==JoinRelType.INTERVAL) {
                throw new IllegalArgumentException("Interval joins are only supported between two streams: " + logicalJoin);
            }
        }

        //If we don't detect a special time-based join, a DEFAULT join is an INNER join
        if (joinType==JoinRelType.DEFAULT) {
            joinType = JoinRelType.INNER;
        }

        if (estimateRowCount(leftInputF.relNode)>cardinalityJoinThreshold ||
            estimateRowCount(rightInputF.relNode)>cardinalityJoinThreshold) {
            combinedMaterialize = combinedMaterialize.update(MaterializationPreference.SHOULD_NOT,"high cardinality join");
        }

        Preconditions.checkArgument(joinType == JoinRelType.INNER, "Unsupported join type: %s", logicalJoin);
        //Default inner join creates a state table
        RelNode newJoin = relB.push(leftInputF.relNode).push(rightInputF.relNode)
                .join(JoinRelType.INNER, condition).build();
        return setRelHolder(new RelMeta(newJoin, TableType.STATE,
                concatPk, TimestampHolder.Derived.NONE, joinedIndexMap,
                null, combinedMaterialize, NowFilter.EMPTY, Deduplication.EMPTY));
    }

    private static<T,R> R apply2JoinSide(int joinIndex, int leftSideMaxIdx, T left, T right, BiFunction<T,Integer,R> function) {
        int idx;
        if (joinIndex>=leftSideMaxIdx) {
            idx = joinIndex-leftSideMaxIdx;
            return function.apply(right,idx);
        } else {
            idx = joinIndex;
            return function.apply(left,idx);
        }
    }

    @Override
    public RelNode visit(LogicalUnion logicalUnion) {
        throw new UnsupportedOperationException("Not yet supported");
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        //Need to inline TopN before we aggregate, but we postpone inlining now-filter in case we can push it through
        final RelMeta input = getRelHolder(aggregate.getInput().accept(this)).inlineDedup();
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

        MaterializationInference materialize = input.materialize;
        if (aggregateCalls.stream().anyMatch(agg -> STREAM_ONLY_AGG.test(agg.getAggregation()))) {
            materialize.update(MaterializationPreference.MUST,"stream-only aggregation function");
        }

        //Check if this is a time-window aggregation
        if (input.type == TableType.STREAM && input.getRelNode() instanceof LogicalProject) {
            //Determine if one of the groupBy keys is a timestamp
            TimestampHolder.Candidate keyCandidate = null;
            int keyIdx = -1;
            for (int i = 0; i < groupByIdx.size(); i++) {
                int idx = groupByIdx.get(i);
                Optional<TimestampHolder.Candidate> candidate = input.timestamp.getCandidateByIndex(idx);
                if (candidate.isPresent()) {
                    Preconditions.checkArgument(keyCandidate==null, "Do not currently support aggregating by multiple timestamp columns");
                    keyCandidate = candidate.get();
                    keyIdx = i;
                    assert keyCandidate.getIndex() == idx;
                }
            }
            if (keyCandidate!=null) {
                LogicalProject inputProject = (LogicalProject)input.getRelNode();
                RexNode timeAgg = inputProject.getProjects().get(keyCandidate.getIndex());
                Optional<TimeTumbleFunctionCall> bucketFct = rexUtil.getTimeBucketingFunction(timeAgg);
                Preconditions.checkArgument(!bucketFct.isEmpty(), "Not a valid time aggregation function: %s", timeAgg);

                //Fix timestamp (if not already fixed)
                TimestampHolder.Derived newTimestamp = input.timestamp.fixTimestamp(keyCandidate.getIndex(), keyIdx);
                //Now filters must be on the timestamp - this is an internal check
                Preconditions.checkArgument(input.nowFilter.isEmpty() || input.nowFilter.getTimestampIndex()==keyCandidate.getIndex());
                NowFilter nowFilter = input.nowFilter.remap(IndexMap.singleton(keyCandidate.getIndex(),keyIdx));

                RelBuilder relB = relBuilderFactory.get();
                relB.push(input.relNode);
                relB.aggregate(relB.groupKey(ImmutableBitSet.of(groupByIdx)),aggregateCalls);
                new TimeAggregationHint(TimeAggregationHint.Type.TUMBLE,keyCandidate.getIndex()).addTo(relB);
                ContinuousIndexMap pk = ContinuousIndexMap.identity(groupByIdx.size(), targetLength);
                ContinuousIndexMap indexMap = ContinuousIndexMap.identity(targetLength, targetLength);

                materialize = materialize.update(MaterializationPreference.MUST,"time-window aggregation");
                /* TODO: this type of streaming aggregation requires a post-filter in the database (in physical model) to filter out "open" time buckets,
                i.e. time_bucket_col < time_bucket_function(now()) [if now() lands in a time bucket, that bucket is still open and shouldn't be shown]
                  set to "SHOULD" once this is supported
                 */

                return setRelHolder(new RelMeta(relB.build(), TableType.STREAM, pk,
                        newTimestamp, indexMap,null, materialize, nowFilter, Deduplication.EMPTY));

            }
        }

        //Check if we need to propagate timestamps
        if (input.type == TableType.STREAM || input.type == TableType.TEMPORAL_STATE) {
            targetLength += 1;

            boolean isSlidingAggregate = false;
            RelMeta nowInput = input;
            if (!input.nowFilter.isEmpty()) {
                nowInput = input.inlineNowFilter();
                isSlidingAggregate = true;
                // TODO: extract slide-width from hint
            }

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
                RexNode agg = rexUtil.getBuilder().makeOver(call.getType(),call.getAggregation(),
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
            if (isSlidingAggregate) new TimeAggregationHint(TimeAggregationHint.Type.SLIDING, candidate.getIndex()).addTo(relB);
            ContinuousIndexMap pk = ContinuousIndexMap.identity(groupByIdx.size(), targetLength);
            ContinuousIndexMap indexMap = ContinuousIndexMap.identity(targetLength-1, targetLength);
            return setRelHolder(new RelMeta(relB.build(), TableType.TEMPORAL_STATE, pk,
                    addedTimestamp, indexMap,null, nowInput.materialize, NowFilter.EMPTY, Deduplication.EMPTY));
        } else {

            //Standard aggregation produces a state table
            Preconditions.checkArgument(input.nowFilter.isEmpty(),"State table cannot have now-filter since there is no timestamp");
            RelBuilder relB = relBuilderFactory.get();
            relB.push(input.relNode);
            relB.aggregate(relB.groupKey(ImmutableBitSet.of(groupByIdx)), aggregateCalls);
            //since there is no timestamp, we cannot propagate a sliding window
            //if (isSlidingAggregate) new TimeAggregationHint(TimeAggregationHint.Type.SLIDING).addTo(relB);
            ContinuousIndexMap pk = ContinuousIndexMap.identity(groupByIdx.size(), targetLength);
            ContinuousIndexMap indexMap = ContinuousIndexMap.identity(targetLength, targetLength);
            return setRelHolder(new RelMeta(relB.build(), TableType.STATE, pk,
                    TimestampHolder.Derived.NONE, indexMap, null, input.materialize, NowFilter.EMPTY, Deduplication.EMPTY));
        }
    }

    @Override
    public RelNode visit(LogicalSort logicalSort) {
        throw new UnsupportedOperationException("Pulling up sorts yet implemented");
//        Preconditions.checkArgument(logicalSort.offset == null, "OFFSET not yet supported");
//        RelMeta input = getRelHolder(logicalSort.getInput().accept(this));
//        Preconditions.checkArgument(!input.dedup.hasPartition(),"Sorting on top of a partitioned relation is invalid");
//        input = input.inlineDedup();
//        if (input.dedup.isDistinct() || input.dedup.hasLimit()) {
//            //Need to inline before we can sort on top
//            input = input.inlineDedup();
//        } //else there is only a sort which we replace by this sort if present
//
//        RelCollation collation = logicalSort.getCollation();
//        //Map the collation fields
//        ContinuousIndexMap indexMap = input.indexMap;
//        RelCollation newCollation = RelCollations.of(collation.getFieldCollations().stream()
//                .map(fc -> fc.withFieldIndex(indexMap.map(fc.getFieldIndex()))).collect(Collectors.toList()));
//        if (newCollation.getFieldCollations().isEmpty()) newCollation = input.dedup.getCollation();
//
//        TopNConstraint topN = new TopNConstraint(newCollation, List.of(), getLimit(logicalSort.fetch), false);
//
//        return setRelHolder(new RelMeta(input.relNode,input.type,input.primaryKey,
//                input.timestamp,input.indexMap,input.joinTables, input.nowFilter, topN));
    }

    public Optional<Integer> getLimit(RexNode limit) {
        Preconditions.checkArgument(limit instanceof RexLiteral);
        return Optional.of(((RexLiteral)limit).getValueAs(Integer.class));
    }




}
