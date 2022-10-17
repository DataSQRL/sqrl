package ai.datasqrl.plan.calcite.rules;

import ai.datasqrl.function.SqrlAwareFunction;
import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.calcite.hints.*;
import ai.datasqrl.plan.calcite.table.*;
import ai.datasqrl.plan.calcite.util.*;
import ai.datasqrl.plan.global.MaterializationPreference;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
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

    private static final long UPPER_BOUND_INTERVAL_MS = 999L*365L*24L*3600L; //999 years
    public static final double HIGH_CARDINALITY_JOIN_THRESHOLD = 10000;
    public static final double DEFAULT_SLIDING_WIDTH_PERCENTAGE = 0.02;
    public static final String UNION_TIMESTAMP_COLUMN_NAME = "_timestamp";

    Supplier<RelBuilder> relBuilderFactory;
    SqrlRexUtil rexUtil;
    MaterializationPreference materializationPreference;
    double cardinalityJoinThreshold = HIGH_CARDINALITY_JOIN_THRESHOLD;
    double defaultSlidePercentage = DEFAULT_SLIDING_WIDTH_PERCENTAGE;

    public SQRLLogicalPlanConverter(Supplier<RelBuilder> relBuilderFactory, Optional<MaterializationPreference> materializationPreference) {
        this.relBuilderFactory = relBuilderFactory;
        this.rexUtil = new SqrlRexUtil(relBuilderFactory.get().getTypeFactory());
        this.materializationPreference = materializationPreference.orElse(MaterializationPreference.SHOULD);
    }

    private RelBuilder makeRelBuilder() {
        return relBuilderFactory.get();
    }

    @Value
    @AllArgsConstructor
    @Builder
    public static class RelMeta implements RelHolder {

        @NonNull RelNode relNode;
        TableType type;
        @NonNull ContinuousIndexMap primaryKey;
        @NonNull TimestampHolder.Derived timestamp;
        @NonNull ContinuousIndexMap select;
        @NonNull MaterializationInference materialize;

        @Builder.Default
        List<JoinTable> joinTables = null;
        @Builder.Default
        Optional<Integer> numRootPks = Optional.empty();

        @Builder.Default @NonNull
        NowFilter nowFilter = NowFilter.EMPTY; //Applies before topN
        @Builder.Default @NonNull
        TopNConstraint topN = TopNConstraint.EMPTY; //Applies before sort
        @Builder.Default @NonNull
        SortOrder sort = SortOrder.EMPTY;

        @Builder.Default
        TableStatistic statistic = null;

        public static RelMetaBuilder build(RelNode relNode, TableType type, ContinuousIndexMap primaryKey,
                TimestampHolder.Derived timestamp, ContinuousIndexMap select, MaterializationInference materialize) {
            return RelMeta.builder().relNode(relNode).type(type).primaryKey(primaryKey).timestamp(timestamp)
                    .select(select).materialize(materialize);
        }

        public RelMetaBuilder copy() {
            RelMetaBuilder builder = RelMeta.builder();
            builder.relNode(relNode);
            builder.type(type);
            builder.primaryKey(primaryKey);
            builder.timestamp(timestamp);
            builder.select(select);
            builder.joinTables(joinTables);
            builder.numRootPks(numRootPks);
            builder.materialize(materialize);
            builder.nowFilter(nowFilter);
            builder.topN(topN);
            builder.sort(sort);
            builder.statistic(statistic);
            return builder;
        }

        /**
         * Called to inline the TopNConstraint on top of the input relation.
         * This will inline a nowFilter if present
         * @return
         */
        public RelMeta inlineTopN(RelBuilder relBuilder) {
            if (topN.isEmpty()) return this;
            if (!nowFilter.isEmpty()) return inlineNowFilter(relBuilder).inlineTopN(relBuilder);

            Preconditions.checkArgument(nowFilter.isEmpty());

            relBuilder.push(relNode);

            MaterializationInference newMaterialize = materialize;
            SortOrder newSort = sort;
            if (!topN.isDistinct() && (!topN.hasPartition() || !topN.hasLimit())) {
                assert topN.hasCollation();
                RelCollation collation = topN.getCollation();
                if (topN.hasLimit()) { //It's not partitioned, so straight forward order and limit
                    relBuilder.sort(collation);
                    relBuilder.limit(0, topN.getLimit());
                    newMaterialize = materialize.update(MaterializationPreference.SHOULD_NOT, "sort");
                } else { //Lift up sort and prepend partition (if any)
                    newSort = newSort.ifEmpty(SortOrder.of(topN.getPartition(),collation));
                }
                return RelMeta.build(relBuilder.build(),type,primaryKey,timestamp,select,newMaterialize)
                        .sort(newSort).build();
            } else { //distinct or (hasPartition and hasLimit)
                final RelDataType inputType = relBuilder.peek().getRowType();
                RexBuilder rexBuilder = relBuilder.getRexBuilder();

                List<Integer> projectIdx = ContiguousSet.closedOpen(0,inputType.getFieldCount()).asList();
                List<Integer> partitionIdx = topN.getPartition();
                if (topN.isDistinct() && !topN.hasPartition()) { //Partition by all distinct columns
                    partitionIdx = primaryKey.targetsAsList();
                }

                int rowFunctionColumns = 1;
                if (topN.isDistinct() && topN.hasPartition()) {
                    rowFunctionColumns += topN.hasLimit()?2:1;
                }
                int targetLength = projectIdx.size()+rowFunctionColumns;

                List<RexNode> partitionKeys = new ArrayList<>(partitionIdx.size());
                List<RexNode> projects = new ArrayList<>(targetLength);
                List<String> projectNames = new ArrayList<>(targetLength);
                for (Integer idx : projectIdx) {
                    RexInputRef ref = RexInputRef.of(idx, inputType);
                    projects.add(ref);
                    projectNames.add(null);
                    if (partitionIdx.contains(idx)) partitionKeys.add(ref);
                }
                assert projects.size() == projectIdx.size() && partitionKeys.size() == partitionIdx.size();

                List<RexFieldCollation> fieldCollations = new ArrayList<>();
                fieldCollations.addAll(SqrlRexUtil.translateCollation(topN.getCollation(),inputType));
                if (topN.isDistinct() && topN.hasPartition()) {
                    //Add all other selects that are not partition indexes or collations to the sort
                    List<Integer> remainingDistincts = new ArrayList<>(primaryKey.targetsAsList());
                    topN.getCollation().getFieldCollations().stream().map(RelFieldCollation::getFieldIndex).forEach(remainingDistincts::remove);
                    topN.getPartition().stream().forEach(remainingDistincts::remove);
                    remainingDistincts.stream().map(idx -> new RexFieldCollation(RexInputRef.of(idx, inputType), Set.of(SqlKind.NULLS_LAST)))
                            .forEach(fieldCollations::add);
                }

                SqrlRexUtil rexUtil = new SqrlRexUtil(relBuilder.getTypeFactory());
                //Add row_number (since it always applies)
                projects.add(rexUtil.createRowFunction(SqlStdOperatorTable.ROW_NUMBER,partitionKeys,fieldCollations));
                projectNames.add(null);
                int rowNumberIdx = projectIdx.size(), rankIdx = rowNumberIdx+1, denserankIdx = rowNumberIdx+2;
                if (topN.isDistinct() && topN.hasPartition()) {
                    //Add rank and dense_rank if we have a limit
                    projects.add(rexUtil.createRowFunction(SqlStdOperatorTable.RANK,partitionKeys,fieldCollations));
                    projectNames.add(null);
                    if (topN.hasLimit()) {
                        projects.add(rexUtil.createRowFunction(SqlStdOperatorTable.DENSE_RANK, partitionKeys, fieldCollations));
                        projectNames.add(null);
                    }
                }

                relBuilder.project(projects, projectNames);
                RelDataType windowType = relBuilder.peek().getRowType();
                //Add filter
                List<RexNode> conditions = new ArrayList<>();
                if (topN.isDistinct() && topN.hasPartition()) {
                    conditions.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, RexInputRef.of(rowNumberIdx, windowType),
                            RexInputRef.of(rankIdx, windowType)));
                    if (topN.hasLimit()) {
                        conditions.add(SqrlRexUtil.makeWindowLimitFilter(rexBuilder, topN.getLimit(), denserankIdx, windowType));
                    }
                } else {
                    final int limit;
                    if (topN.isDistinct()) {
                        assert !topN.hasLimit();
                        limit = 1;
                    } else {
                        limit = topN.getLimit();
                    }
                    conditions.add(SqrlRexUtil.makeWindowLimitFilter(rexBuilder, limit, rowNumberIdx, windowType));
                }


                relBuilder.filter(conditions);
                if (topN.hasPartition() && (!topN.hasLimit() || topN.getLimit()>1)) {
                    //Add sort on top
                    SortOrder sortByRowNum = SortOrder.of(topN.getPartition(),RelCollations.of(new RelFieldCollation(rowNumberIdx,
                            RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST)));
                    newSort = newSort.ifEmpty(sortByRowNum);
                }
                ContinuousIndexMap newPk = primaryKey.remap(targetLength, IndexMap.IDENTITY);
                ContinuousIndexMap newSelect = select.remap(targetLength, IndexMap.IDENTITY);
                return RelMeta.build(relBuilder.build(),type,newPk,timestamp,newSelect,newMaterialize)
                        .sort(newSort).build();
            }
        }

        public RelMeta inlineNowFilter(RelBuilder relB) {
            if (nowFilter.isEmpty()) return this;
            nowFilter.addFilterTo(relB.push(relNode));
            return copy().relNode(relB.build())
                    .materialize(materialize.update(MaterializationPreference.CANNOT, "now-filter"))
                    .nowFilter(NowFilter.EMPTY).build();

        }

        public RelMeta inlineSort(RelBuilder relB) {
            if (sort.isEmpty()) return this;
            //Need to inline now-filter and topN first
            if (!nowFilter.isEmpty()) return inlineNowFilter(relB).inlineSort(relB);
            if (!topN.isEmpty()) return inlineTopN(relB).inlineSort(relB);
            sort.addTo(relB.push(relNode));
            return copy().relNode(relB.build())
                    .materialize(materialize.update(MaterializationPreference.SHOULD_NOT, "sort"))
                    .sort(SortOrder.EMPTY).build();
        }

        public boolean hasPullups() {
            return !topN.isEmpty() || !nowFilter.isEmpty() || !sort.isEmpty();
        }

        public PullupOperator.Container getPullups() {
            return new PullupOperator.Container(nowFilter,topN,sort);
        }

    }

    /**
     * Moves the primary key columns to the front and adds projection to only return
     * columns that the user selected, are part of the primary key, a timestamp candidate, or
     * part of the sort order.
     *
     * Inlines deduplication in case of nested data.
     *
     * @param input
     * @return
     */
    public RelMeta postProcess(RelMeta input, List<String> fieldNames) {
        Preconditions.checkArgument(fieldNames.size()==input.select.getSourceLength());
        List<RelDataTypeField> fields = input.getRelNode().getRowType().getFieldList();
        if (!input.getTopN().isEmpty() && //If any selected field is nested we have to inline topN
                input.select.targetsAsList().stream().map(fields::get).map(RelDataTypeField::getType).anyMatch(CalciteUtil::isNestedTable)) {
            input = input.inlineTopN(makeRelBuilder());
        }
        ContinuousIndexMap indexMap = input.select;
        HashMap<Integer,Integer> remapping = new HashMap<>();
        int index = 0;
        for (int i = 0; i < input.primaryKey.getSourceLength(); i++) {
            remapping.put(input.primaryKey.map(i),index++);
        }
        for (int i = 0; i < input.select.getSourceLength(); i++) {
            int target = input.select.map(i);
            if (!remapping.containsKey(target)) {
                remapping.put(target,index++);
            }
        }
        for (TimestampHolder.Candidate c : input.timestamp.getCandidates()) {
            if (!remapping.containsKey(c.getIndex())) {
                remapping.put(c.getIndex(),index++);
            }
        }
        for (RelFieldCollation fieldcol : input.sort.getCollation().getFieldCollations()) {
            if (!remapping.containsKey(fieldcol.getFieldIndex())) {
                remapping.put(fieldcol.getFieldIndex(),index++);
            }
        }

        Preconditions.checkArgument(index<=indexMap.getTargetLength() && remapping.size() == index);
        IndexMap remap = IndexMap.of(remapping);
        ContinuousIndexMap updatedIndexMap = input.select.remap(remap);
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

        return new RelMeta(relNode,input.type,input.primaryKey.remap(remap),
                input.timestamp.remapIndexes(remap), updatedIndexMap, input.materialize, null, input.numRootPks,
                input.nowFilter.remap(remap), input.topN.remap(remap), input.sort.remap(remap), statistic);
    }

    private RelMeta postProcess(RelMeta input) {
        return postProcess(input,Collections.nCopies(input.select.getSourceLength(),null));
    }

    private double estimateRowCount(RelNode node) {
        final RelMetadataQuery mq = node.getCluster().getMetadataQuery();
        return mq.getRowCount(node);
    }

    public RelMeta distinctOn(RelMeta input) {
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
        VirtualRelationalTable.Root root = vtable.getRoot();
        QueryRelationalTable queryTable = root.getBase();
        AtomicReference<MaterializationInference> materialize = new AtomicReference<>(new MaterializationInference(
                queryTable.getMaterialization().getPreference()==MaterializationPreference.CANNOT?
                MaterializationPreference.CANNOT:materializationPreference));
        //Now, we shred
        RelNode relNode = shredTable(vtable, primaryKey, indexMap, joinTables, materialize, true).build();
        //Finally, we assemble the result

        int mapToLength = relNode.getRowType().getFieldCount();
        Optional<Integer> numRootPks = Optional.of(vtable.getRoot().getNumPrimaryKeys());
        RelMeta result = new RelMeta(relNode, queryTable.getType(),
                primaryKey.build(mapToLength),
                queryTable.getTimestamp().getDerived(),
                indexMap.build(mapToLength), materialize.get(), joinTables, numRootPks,
                queryTable.getPullups().getNowFilter(), queryTable.getPullups().getTopN(),
                queryTable.getPullups().getSort(), null);
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
            QueryRelationalTable qTable = root.getBase();
            builder.scan(root.getBase().getNameId());
            CalciteUtil.addIdentityProjection(builder,root.getNumQueryColumns());
            joinTable = JoinTable.ofRoot(root);
            if (qTable.getMaterialization().getPreference()==MaterializationPreference.CANNOT) {
                materialize.getAndUpdate(m -> m.update(MaterializationPreference.CANNOT,"input table"));
            }
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
        input = input.inlineTopN(makeRelBuilder()); //Filtering doesn't preserve deduplication
        RexNode condition = logicalFilter.getCondition();
        condition = SqrlRexUtil.mapIndexes(condition,input.select);
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
            Optional<NowFilter> combinedFilter = NowFilter.of(timeFunctions);
            Optional<NowFilter> resultFilter = combinedFilter.flatMap(nowFilter::merge);
            Preconditions.checkArgument(resultFilter.isPresent(),"Unsatisfiable now-filter detected");
            nowFilter = resultFilter.get();
            int timestampIdx = nowFilter.getTimestampIndex();
            timestamp = timestamp.getCandidateByIndex(timestampIdx).fixAsTimestamp();
            //Add as static time filter (to push down to source)
            NowFilter localNowFilter = combinedFilter.get();
            //TODO: add back in, push down to push into source, then remove
            //localNowFilter.addFilterTo(relBuilder,true);
        } else {
            conjunctions = List.of(condition);
        }
        relBuilder.filter(conjunctions);
        return setRelHolder(input.copy().relNode(relBuilder.build()).timestamp(timestamp)
                .materialize(analyzeRexNodesForMaterialize(input.materialize, conjunctions))
                .nowFilter(nowFilter).build());
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

        ContinuousIndexMap trivialMap = getTrivialMapping(logicalProject, rawInput.select);
        if (trivialMap!=null) {
            //Check if this is a topN constraint
            Optional<TopNHint> topNHintOpt = SqrlHint.fromRel(logicalProject, TopNHint.CONSTRUCTOR);
            if (topNHintOpt.isPresent()) {
                TopNHint topNHint = topNHintOpt.get();

                RelNode base = logicalProject.getInput();
                RelCollation collation = RelCollations.EMPTY;
                Optional<Integer> limit = Optional.empty();
                if (base instanceof LogicalSort) {
                    LogicalSort nestedSort = (LogicalSort)base;
                    base = nestedSort.getInput();
                    collation = nestedSort.getCollation();
                    limit = getLimit(nestedSort.fetch);
                }

                RelMeta baseInput = getRelHolder(base.accept(this));
                baseInput = baseInput.inlineNowFilter(makeRelBuilder()).inlineTopN(makeRelBuilder());
                int targetLength = baseInput.select.getTargetLength();

                collation = SqrlRexUtil.mapCollation(collation, baseInput.select);
                if (collation.getFieldCollations().isEmpty()) collation = baseInput.sort.getCollation();
                List<Integer> partition = topNHint.getPartition().stream().map(baseInput.select::map).collect(Collectors.toList());

                ContinuousIndexMap pk = baseInput.primaryKey;
                ContinuousIndexMap select = trivialMap;
                TimestampHolder.Derived timestamp = baseInput.timestamp;
                TableType type = baseInput.type==TableType.STATE?TableType.STATE:TableType.TEMPORAL_STATE;
                boolean isDistinct = false;
                if (topNHint.getType() == TopNHint.Type.SELECT_DISTINCT) {
                    isDistinct = true;
                    List<Integer> distincts = SqrlRexUtil.combineIndexes(partition, trivialMap.targetsAsList());
                    pk = ContinuousIndexMap.builder(distincts.size()).addAll(distincts).build(targetLength);
                    if (partition.isEmpty()) {
                        //If there is no partition, we can ignore the sort order plus limit and sort by time instead
                        timestamp = timestamp.getBestCandidate().fixAsTimestamp();
                        collation = RelCollations.of(new RelFieldCollation(timestamp.getTimestampCandidate().getIndex(),
                                RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));
                        limit = Optional.empty();
                    }
                } else if (topNHint.getType() == TopNHint.Type.DISTINCT_ON) {
                    //Partition is the new primary key and the underlying table must be a stream
                    Preconditions.checkArgument(!partition.isEmpty() && collation.getFieldCollations().size()==1 && baseInput.type==TableType.STREAM);
                    isDistinct = true;

                    pk = ContinuousIndexMap.builder(partition.size()).addAll(partition).build(targetLength);
                    select = ContinuousIndexMap.identity(targetLength,targetLength); //Select everything
                    //Extract timestamp from collation
                    RelFieldCollation fieldCol = Iterables.getOnlyElement(collation.getFieldCollations())
                            .withNullDirection(RelFieldCollation.NullDirection.LAST); //overwrite null-direction
                    Preconditions.checkArgument(fieldCol.direction == RelFieldCollation.Direction.DESCENDING &&
                            fieldCol.nullDirection == RelFieldCollation.NullDirection.LAST);
                    collation = RelCollations.of(fieldCol);
                    timestamp = timestamp.getCandidateByIndex(fieldCol.getFieldIndex()).fixAsTimestamp();
                    partition = Collections.EMPTY_LIST; //remove partition since we set primary key to partition
                    limit = Optional.empty(); //distinct does not need a limit
                } else if (topNHint.getType() == TopNHint.Type.TOP_N) {
                    //Prepend partition to primary key
                    List<Integer> pkIdx = SqrlRexUtil.combineIndexes(partition, pk.targetsAsList());
                    pk = ContinuousIndexMap.builder(pkIdx.size()).addAll(pkIdx).build(targetLength);
                }

                TopNConstraint topN = new TopNConstraint(partition,isDistinct,collation,limit);
                return setRelHolder(baseInput.copy().type(type).primaryKey(pk).select(select).timestamp(timestamp)
                        .joinTables(null).topN(topN).sort(SortOrder.EMPTY).build());
            } else {
                //If it's a trivial project, we remove it and only update the indexMap. This is needed to eliminate self-joins
                return setRelHolder(rawInput.copy().select(trivialMap).build());
            }
        }
        RelMeta input = rawInput.inlineTopN(makeRelBuilder());
        //Update index mappings
        List<RexNode> updatedProjects = new ArrayList<>();
        List<String> updatedNames = new ArrayList<>();
        //We only keep track of the first mapped project and consider it to be the "preserving one" for primary keys and timestamps
        Map<Integer,Integer> mappedProjects = new HashMap<>();
        List<TimestampHolder.Derived.Candidate> timeCandidates = new ArrayList<>();
        NowFilter nowFilter = NowFilter.EMPTY;
        for (Ord<RexNode> exp : Ord.<RexNode>zip(logicalProject.getProjects())) {
            RexNode mapRex = SqrlRexUtil.mapIndexes(exp.e,input.select);
            updatedProjects.add(exp.i,mapRex);
            updatedNames.add(exp.i,logicalProject.getRowType().getFieldNames().get(exp.i));
            int originalIndex = -1;
            if (mapRex instanceof RexInputRef) { //Direct mapping
                originalIndex = (((RexInputRef) mapRex)).getIndex();
            } else { //Check for preserved timestamps
                Optional<TimestampHolder.Derived.Candidate> preservedCandidate = rexUtil.getPreservedTimestamp(mapRex, input.timestamp);
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
                            input = input.inlineNowFilter(makeRelBuilder());
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
            if (target==null) {
                //Need to add it
                target = updatedProjects.size();
                updatedProjects.add(target,RexInputRef.of(p.getTarget(),input.relNode.getRowType()));
                updatedNames.add(null);
                mappedProjects.put(p.getTarget(),target);
            }
            primaryKey.add(target);
        }
        for (TimestampHolder.Derived.Candidate candidate : input.timestamp.getCandidates()) {
            //Check if candidate is already mapped through timestamp preserving function
            if (timeCandidates.contains(candidate)) continue;
            Integer target = mappedProjects.get(candidate.getIndex());
            if (target==null) {
                //Need to add candidate
                target = updatedProjects.size();
                updatedProjects.add(target,RexInputRef.of(candidate.getIndex(),input.relNode.getRowType()));
                updatedNames.add(null);
                mappedProjects.put(candidate.getIndex(),target);
            } else {
                //Update now-filter if it matches candidate
                if (!input.nowFilter.isEmpty() && input.nowFilter.getTimestampIndex() == candidate.getIndex()) {
                    nowFilter = input.nowFilter.remap(IndexMap.singleton(candidate.getIndex(), target));
                }
            }
            timeCandidates.add(candidate.withIndex(target));
        }
        TimestampHolder.Derived timestamp = input.timestamp.restrictTo(timeCandidates);
        //NowFilter must have been preserved
        assert !nowFilter.isEmpty() || input.nowFilter.isEmpty();

        //TODO: preserve sort
        List<RelFieldCollation> collations = new ArrayList<>(input.sort.getCollation().getFieldCollations());
        for (int i = 0; i < collations.size(); i++) {
            RelFieldCollation fieldcol = collations.get(i);
            Integer target = mappedProjects.get(fieldcol.getFieldIndex());
            if (target==null) {
                //Need to add candidate
                target = updatedProjects.size();
                updatedProjects.add(target,RexInputRef.of(fieldcol.getFieldIndex(),input.relNode.getRowType()));
                updatedNames.add(null);
                mappedProjects.put(fieldcol.getFieldIndex(),target);
            }
            collations.set(i,fieldcol.withFieldIndex(target));
        }
        SortOrder sort = new SortOrder(RelCollations.of(collations));

        //Build new project
        RelBuilder relB = relBuilderFactory.get();
        relB.push(input.relNode);
        relB.project(updatedProjects,updatedNames);
        RelNode newProject = relB.build();
        int fieldCount = updatedProjects.size();
        return setRelHolder(RelMeta.build(newProject,input.type,primaryKey.build(fieldCount),
                timestamp, ContinuousIndexMap.identity(logicalProject.getProjects().size(),fieldCount),
                analyzeRexNodesForMaterialize(input.materialize,updatedProjects)).numRootPks(input.numRootPks)
                .nowFilter(nowFilter).sort(sort).build());
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
        RelMeta leftIn = getRelHolder(logicalJoin.getLeft().accept(this));
        RelMeta rightIn = getRelHolder(logicalJoin.getRight().accept(this));

        //TODO: pull now-filters through if possible
        RelMeta leftInput = leftIn.inlineNowFilter(makeRelBuilder()).inlineTopN(makeRelBuilder());
        RelMeta rightInput = rightIn.inlineNowFilter(makeRelBuilder()).inlineTopN(makeRelBuilder());
        JoinRelType joinType = logicalJoin.getJoinType();

        ContinuousIndexMap joinedIndexMap = leftInput.select.join(rightInput.select);
        RexNode condition = SqrlRexUtil.mapIndexes(logicalJoin.getCondition(),joinedIndexMap);
        //TODO: pull now() conditions up as a nowFilter and move nested now filters through
        Preconditions.checkArgument(!FIND_NOW.foundIn(condition),"now() is not allowed in join conditions");
        SqrlRexUtil.EqualityComparisonDecomposition eqDecomp = rexUtil.decomposeEqualityComparison(condition);

        final int leftSideMaxIdx = leftInput.select.getTargetLength();

        //Identify if this is an identical self-join for a nested tree
        boolean hasPullups = leftIn.hasPullups() || rightIn.hasPullups();
        if ((joinType==JoinRelType.DEFAULT || joinType==JoinRelType.INNER) && leftInput.joinTables!=null && rightInput.joinTables!=null
                && !hasPullups && eqDecomp.getRemainingPredicates().isEmpty()) {
            //Determine if we can map the tables from both branches of the join onto each-other
            int leftTargetLength = leftInput.select.getTargetLength();
            Map<JoinTable, JoinTable> right2left = JoinTable.joinTreeMap(leftInput.joinTables,
                    leftTargetLength , rightInput.joinTables, eqDecomp.getEqualities());
            if (!right2left.isEmpty()) {
                //We currently expect a single path from leaf to right as a self-join
                Preconditions.checkArgument(JoinTable.getRoots(rightInput.joinTables).size() == 1, "Current simplifying assumption");
                JoinTable rightLeaf = Iterables.getOnlyElement(JoinTable.getLeafs(rightInput.joinTables));
                RelBuilder relBuilder = relBuilderFactory.get().push(leftInput.getRelNode());
                ContinuousIndexMap newPk = leftInput.primaryKey;
                List<JoinTable> joinTables = new ArrayList<>(leftInput.joinTables);
                AtomicReference<MaterializationInference> materialize = new AtomicReference<>(
                        leftInput.materialize.combine(rightInput.materialize));
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
                ContinuousIndexMap remapedRight = rightInput.select.remap(relNode.getRowType().getFieldCount(),
                        index -> {
                            JoinTable jt = JoinTable.find(rightInputfinal.joinTables,index).get();
                            return right2left.get(jt).getGlobalIndex(jt.getLocalIndex(index));
                        });
                ContinuousIndexMap indexMap = leftInput.select.append(remapedRight);
                return setRelHolder(RelMeta.build(relNode, leftInput.type, newPk, leftInput.timestamp,
                        indexMap, materialize.get()).numRootPks(leftInput.numRootPks).joinTables(joinTables).build());

            }
        }

        MaterializationInference combinedMaterialize = leftInput.materialize.combine(rightInput.materialize);
        combinedMaterialize = analyzeRexNodesForMaterialize(combinedMaterialize,List.of(condition));

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

                    int tmpLeftSideMaxIdx = leftInput.select.getTargetLength();
                    IndexMap leftRightFlip = idx -> idx<leftSideMaxIdx?tmpLeftSideMaxIdx+idx:idx-leftSideMaxIdx;
                    condition = SqrlRexUtil.mapIndexes(logicalJoin.getCondition(), leftRightFlip);
                    joinedIndexMap = joinedIndexMap.remap(leftRightFlip);
                    eqDecomp = rexUtil.decomposeEqualityComparison(condition);
                }
                int newLeftSideMaxIdx = leftInput.select.getTargetLength();
                //Check for primary keys equalities on the state-side of the join
                Set<Integer> pkIndexes = rightInput.primaryKey.getMapping().stream().map(p-> p.getTarget()+newLeftSideMaxIdx).collect(Collectors.toSet());
                Set<Integer> pkEqualities = eqDecomp.getEqualities().stream().map(p -> p.target).collect(Collectors.toSet());
                if (pkIndexes.equals(pkEqualities) && eqDecomp.getRemainingPredicates().isEmpty()) {
                    joinType = JoinRelType.TEMPORAL;
                    RelBuilder relB = relBuilderFactory.get();
                    relB.push(leftInput.relNode); relB.push(rightInput.relNode);
                    Preconditions.checkArgument(rightInput.timestamp.hasFixedTimestamp());
                    TimestampHolder.Derived joinTimestamp = leftInput.timestamp.getBestCandidate().fixAsTimestamp();

                    ContinuousIndexMap pk = ContinuousIndexMap.builder(leftInput.primaryKey,0)
                            .build(joinedIndexMap.getTargetLength());
                    TemporalJoinHint hint = new TemporalJoinHint(joinTimestamp.getTimestampCandidate().getIndex(),
                            rightInput.timestamp.getTimestampCandidate().getIndex(),
                            rightInput.primaryKey.targetsAsArray());
                    relB.join(JoinRelType.INNER, condition);
                    hint.addTo(relB);
                    return setRelHolder(RelMeta.build(relB.build(), TableType.STREAM,
                            pk, joinTimestamp, joinedIndexMap,
                            combinedMaterialize.update(MaterializationPreference.MUST,"temporal join"))
                            .numRootPks(leftInput.numRootPks).sort(leftInput.sort).build());
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
        concatPkBuilder.addAll(rightInputF.primaryKey.remap(joinedIndexMap.getTargetLength(), idx -> idx + leftSideMaxIdx));
        ContinuousIndexMap concatPk = concatPkBuilder.build(joinedIndexMap.getTargetLength());

        //combine sorts if present
        SortOrder joinedSort = leftInputF.sort.join(rightInputF.sort.remap(idx -> idx+leftSideMaxIdx));

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
                Optional<Integer> numRootPks = Optional.empty();
                if (timePredicates.isEmpty() && leftInputF.numRootPks.flatMap(npk ->
                        rightInputF.numRootPks.filter(npk2 -> npk2.equals(npk))).isPresent()) {
                    //If both streams have same number of root primary keys, check if those are part of equality conditiosn
                    List<IntPair> rootPkPairs = new ArrayList<>();
                    for (int i = 0; i < leftInputF.numRootPks.get(); i++) {
                        rootPkPairs.add(new IntPair(leftInputF.primaryKey.map(i), rightInputF.primaryKey.map(i)+leftSideMaxIdx));
                    }
                    if (eqDecomp.getEqualities().containsAll(rootPkPairs)) {
                        //Change primary key to only include root pk once and equality time condition because timestamps must be equal
                        timePredicates.add(new TimePredicate(rightInputF.timestamp.getBestCandidate().getIndex()+leftSideMaxIdx,
                                leftInputF.timestamp.getBestCandidate().getIndex(), false, 0));
                        numRootPks = leftInputF.numRootPks;
                        //remove root pk columns from right side when combining primary keys
                        concatPkBuilder = ContinuousIndexMap.builder(leftInputF.primaryKey,rightInputF.primaryKey.getSourceLength()-numRootPks.get());
                        List<Integer> rightPks = rightInputF.primaryKey.targetsAsList();
                        concatPkBuilder.addAll(rightPks.subList(numRootPks.get(),rightPks.size()).stream().map(idx -> idx + leftSideMaxIdx).collect(Collectors.toList()));
                        concatPk = concatPkBuilder.build(joinedIndexMap.getTargetLength());
                    }
                }
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
                                (prel, idx) -> prel.timestamp.getCandidateByIndex(idx).withIndex(tidx).fixAsTimestamp());
                        if (tidx == upperBoundTimestampIndex) joinTimestamp = newTimestamp;
                    }
                    assert joinTimestamp != null;

                    if (timePredicates.size() == 1 && !timePredicates.get(0).isEquality()) {
                        //We only have an upper bound, add (very loose) bound in other direction - Flink requires this
                        conjunctions = new ArrayList<>(conjunctions);
                        final RexNode findInCondition = condition;
                        conjunctions.add(Iterables.getOnlyElement(timePredicates)
                                .inverseWithInterval(UPPER_BOUND_INTERVAL_MS).createRexNode(rexUtil.getBuilder(),
                                        idx -> SqrlRexUtil.findRexInputRefByIndex(idx).find(findInCondition).get(),false));
                        condition = RexUtil.composeConjunction(rexUtil.getBuilder(), conjunctions);
                    }
                    joinType = JoinRelType.INTERVAL;
                    relB.join(JoinRelType.INNER, condition); //Can treat as "standard" inner join since no modification is necessary in physical plan
                    return setRelHolder(RelMeta.build(relB.build(), TableType.STREAM,
                            concatPk, joinTimestamp, joinedIndexMap,combinedMaterialize).numRootPks(numRootPks).sort(joinedSort).build());
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
        return setRelHolder(RelMeta.build(newJoin, TableType.STATE,
                concatPk, TimestampHolder.Derived.NONE, joinedIndexMap,
                combinedMaterialize).sort(joinedSort).build());
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
        Preconditions.checkArgument(logicalUnion.all,"Currently, only UNION ALL is supported. Combine with SELECT DISTINCT for UNION");

        List<RelMeta> inputs = logicalUnion.getInputs().stream().map(in -> getRelHolder(in.accept(this)).inlineTopN(makeRelBuilder()))
                .map(meta -> meta.copy().sort(SortOrder.EMPTY).build()) //We ignore the sorts of the inputs (if any) since they are streams and we union them the default sort is timestamp
                .map(meta -> postProcess(meta)) //The post-process makes sure the input relations are aligned (pk,selects,timestamps)
                .collect(Collectors.toList());
        Preconditions.checkArgument(inputs.size()>0);

        RelBuilder relBuilder = makeRelBuilder();
        MaterializationInference materialize = null;
        ContinuousIndexMap pk = inputs.get(0).primaryKey;
        ContinuousIndexMap select = inputs.get(0).select;
        Optional<Integer> numRootPks = inputs.get(0).numRootPks;
        int maxSelectIdx = Collections.max(select.targetsAsList());
        List<Integer> selectIndexes = SqrlRexUtil.combineIndexes(pk.targetsAsList(),select.targetsAsList());
        List<String> selectNames = Collections.nCopies(maxSelectIdx,null);
        assert maxSelectIdx == selectIndexes.size() && ContiguousSet.closedOpen(0,maxSelectIdx).asList().equals(selectIndexes) : maxSelectIdx + " vs " + selectIndexes;

        /* Timestamp determination works as follows: First, we collect all candidates that are part of the selected indexes
          and are identical across all inputs. If this set is non-empty, it becomes the new timestamp. Otherwise, we pick
          the best timestamp candidate for each input, fix it, and append it as timestamp.
         */
        Set<Integer> timestampIndexes = inputs.get(0).timestamp.getCandidateIndexes().stream()
                .filter(idx -> idx<maxSelectIdx).collect(Collectors.toSet());
        for (RelMeta input : inputs) {
            Preconditions.checkArgument(input.type==TableType.STREAM,"Only stream tables can currently be unioned. Union tables before converting them to state.");
            //Validate primary key and selects line up between the streams
            Preconditions.checkArgument(pk.equals(input.primaryKey),"Input streams have different primary keys");
            Preconditions.checkArgument(select.equals(input.select),"Input streams select different columns");
            timestampIndexes.retainAll(input.timestamp.getCandidateIndexes());
            numRootPks = numRootPks.flatMap( npk -> input.numRootPks.filter( npk2 -> npk.equals(npk2)));
        }

        TimestampHolder.Derived unionTimestamp = TimestampHolder.Derived.NONE;
        for (RelMeta input : inputs) {
            TimestampHolder.Derived localTimestamp;
            List<Integer> localSelectIndexes = new ArrayList<>(selectIndexes);
            List<String> localSelectNames = new ArrayList<>(selectNames);
            if (timestampIndexes.isEmpty()) { //Pick best and append
                TimestampHolder.Derived.Candidate bestCand = input.timestamp.getBestCandidate();
                localSelectIndexes.add(bestCand.getIndex());
                localSelectNames.add(UNION_TIMESTAMP_COLUMN_NAME);
                localTimestamp = bestCand.withIndex(maxSelectIdx).fixAsTimestamp();
            } else {
                localTimestamp = input.timestamp.restrictTo(input.timestamp.getCandidates().stream()
                        .filter(c -> timestampIndexes.contains(c.getIndex())).collect(Collectors.toList()));
            }

            relBuilder.push(input.relNode);
            CalciteUtil.addProjection(relBuilder, selectIndexes, selectNames);
            unionTimestamp.union(localTimestamp);
        }
        relBuilder.union(true,inputs.size());
        return setRelHolder(RelMeta.build(relBuilder.build(),TableType.STREAM,pk,unionTimestamp,select,materialize)
                .numRootPks(numRootPks).build());
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        //Need to inline TopN before we aggregate, but we postpone inlining now-filter in case we can push it through
        final RelMeta input = getRelHolder(aggregate.getInput().accept(this)).inlineTopN(makeRelBuilder());
        Preconditions.checkArgument(aggregate.groupSets.size()==1,"Do not yet support GROUPING SETS.");
        final List<Integer> groupByIdx = aggregate.getGroupSet().asList().stream()
                .map(idx -> input.select.map(idx))
                .collect(Collectors.toList());
        //We are making the assumption that remapping preserves sort order since group-by indexes must be sorted (they are converted to bitsets)
        Preconditions.checkArgument(groupByIdx.equals(groupByIdx.stream().sorted().collect(Collectors.toList())));
        List<AggregateCall> aggregateCalls = aggregate.getAggCallList().stream().map(agg -> {
            Preconditions.checkArgument(agg.getCollation().getFieldCollations().isEmpty(), "Unexpected aggregate call: %s", agg);
            Preconditions.checkArgument(agg.filterArg<0,"Unexpected aggregate call: %s", agg);
            return agg.copy(agg.getArgList().stream().map(idx -> input.select.map(idx)).collect(Collectors.toList()));
        }).collect(Collectors.toList());
        int targetLength = groupByIdx.size() + aggregateCalls.size();

        MaterializationInference materialize = input.materialize;
        if (aggregateCalls.stream().anyMatch(agg -> STREAM_ONLY_AGG.test(agg.getAggregation()))) {
            materialize.update(MaterializationPreference.MUST,"stream-only aggregation function");
        }

        //Check if this an aggregation of a stream on root primary key
        if (input.type == TableType.STREAM && input.numRootPks.isPresent() && materialize.getPreference().canMaterialize()
                && input.numRootPks.get() <= groupByIdx.size() && groupByIdx.subList(0,input.numRootPks.get())
                .equals(input.primaryKey.targetsAsList().subList(0,input.numRootPks.get()))) {
            TimestampHolder.Derived.Candidate candidate = input.timestamp.getCandidates().stream()
                    .filter(cand -> groupByIdx.contains(cand.getIndex())).findAny().orElse(input.timestamp.getBestCandidate());


            RelBuilder relB = relBuilderFactory.get();
            relB.push(input.relNode);
            Triple<ContinuousIndexMap, ContinuousIndexMap, TimestampHolder.Derived> addedTimestamp =
                    addTimestampAggregate(relB,groupByIdx,candidate,aggregateCalls);
            ContinuousIndexMap pk = addedTimestamp.getLeft(), select = addedTimestamp.getMiddle();
            TimestampHolder.Derived timestamp = addedTimestamp.getRight();

            new TumbleAggregationHint(candidate.getIndex(), TumbleAggregationHint.Type.INSTANT).addTo(relB);

            materialize = materialize.update(MaterializationPreference.MUST,"time-window aggregation");
            NowFilter nowFilter = input.nowFilter.remap(IndexMap.singleton(candidate.getIndex(),
                     timestamp.getTimestampCandidate().getIndex()));

            return setRelHolder(RelMeta.build(relB.build(), TableType.STREAM, pk, timestamp, select, materialize)
                    .numRootPks(Optional.of(pk.getSourceLength()))
                    .nowFilter(nowFilter).build());
        }

        //Check if this is a time-window aggregation
        if (input.type == TableType.STREAM && input.getRelNode() instanceof LogicalProject) {
            //Determine if one of the groupBy keys is a timestamp
            TimestampHolder.Derived.Candidate keyCandidate = null;
            int keyIdx = -1;
            for (int i = 0; i < groupByIdx.size(); i++) {
                int idx = groupByIdx.get(i);
                if (input.timestamp.isCandidate(idx)) {
                    Preconditions.checkArgument(keyCandidate==null, "Do not currently support aggregating by multiple timestamp columns");
                    keyCandidate = input.timestamp.getCandidateByIndex(idx);
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
                TimestampHolder.Derived newTimestamp = keyCandidate.withIndex(keyIdx).fixAsTimestamp();
                //Now filters must be on the timestamp - this is an internal check
                Preconditions.checkArgument(input.nowFilter.isEmpty() || input.nowFilter.getTimestampIndex()==keyCandidate.getIndex());
                NowFilter nowFilter = input.nowFilter.remap(IndexMap.singleton(keyCandidate.getIndex(),keyIdx));

                RelBuilder relB = relBuilderFactory.get();
                relB.push(input.relNode);
                relB.aggregate(relB.groupKey(Ints.toArray(groupByIdx)),aggregateCalls);
                new TumbleAggregationHint(keyCandidate.getIndex(), TumbleAggregationHint.Type.FUNCTION).addTo(relB);
                ContinuousIndexMap pk = ContinuousIndexMap.identity(groupByIdx.size(), targetLength);
                ContinuousIndexMap select = ContinuousIndexMap.identity(targetLength, targetLength);

                materialize = materialize.update(MaterializationPreference.MUST,"time-window aggregation");
                /* TODO: this type of streaming aggregation requires a post-filter in the database (in physical model) to filter out "open" time buckets,
                i.e. time_bucket_col < time_bucket_function(now()) [if now() lands in a time bucket, that bucket is still open and shouldn't be shown]
                  set to "SHOULD" once this is supported
                 */

                return setRelHolder(RelMeta.build(relB.build(), TableType.STREAM, pk, newTimestamp, select, materialize)
                        .numRootPks(Optional.of(pk.getSourceLength()))
                        .nowFilter(nowFilter).build());

            }
        }

        //Check if we need to propagate timestamps
        if (input.type == TableType.STREAM || input.type == TableType.TEMPORAL_STATE) {

            //Fix best timestamp (if not already fixed)
            TimestampHolder.Derived inputTimestamp = input.timestamp;
            TimestampHolder.Derived.Candidate candidate = inputTimestamp.getBestCandidate();
            targetLength += 1; //Adding timestamp column to output relation

            if (!input.nowFilter.isEmpty() && input.materialize.getPreference().isMaterialize()) {
                NowFilter nowFilter = input.nowFilter;
                //Determine timestamp, add to group-By and
                Preconditions.checkArgument(nowFilter.getTimestampIndex()==candidate.getIndex(),"Timestamp indexes don't match");
                Preconditions.checkArgument(!groupByIdx.contains(candidate.getIndex()),"Cannot group on timestamp");

                RelBuilder relB = relBuilderFactory.get();
                relB.push(input.relNode);
                Triple<ContinuousIndexMap, ContinuousIndexMap, TimestampHolder.Derived> addedTimestamp =
                        addTimestampAggregate(relB,groupByIdx,candidate,aggregateCalls);
                ContinuousIndexMap pk = addedTimestamp.getLeft(), select = addedTimestamp.getMiddle();
                TimestampHolder.Derived timestamp = addedTimestamp.getRight();

                //Convert now-filter to sliding window and add as hint
                long intervalWidthMs = nowFilter.getPredicate().getInterval_ms();
                // TODO: extract slide-width from hint
                long slideWidthMs = Math.round(Math.ceil(intervalWidthMs*defaultSlidePercentage));
                Preconditions.checkArgument(slideWidthMs>0 && slideWidthMs<intervalWidthMs,"Invalid window widths: %s - %s",intervalWidthMs,slideWidthMs);
                new SlidingAggregationHint(candidate.getIndex(),intervalWidthMs, slideWidthMs).addTo(relB);

                TopNConstraint dedup = TopNConstraint.dedup(pk.targetsAsList(),timestamp.getTimestampCandidate().getIndex());
                return setRelHolder(RelMeta.build(relB.build(), TableType.TEMPORAL_STATE, pk,
                        timestamp, select, input.materialize.update(MaterializationPreference.MUST,"Sliding window"))
                        .topN(dedup).build());
            } else {
                //Convert aggregation to window-based aggregation in a project so we can preserve timestamp
                RelMeta nowInput = input.inlineNowFilter(makeRelBuilder());

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
                RexFieldCollation orderBy = new RexFieldCollation(timestampRef, Set.of());

                //Add aggregate functions
                for (int i = 0; i < aggregateCalls.size(); i++) {
                    AggregateCall call = aggregateCalls.get(i);
                    RexNode agg = rexUtil.getBuilder().makeOver(call.getType(), call.getAggregation(),
                            call.getArgList().stream()
                                    .map(idx -> RexInputRef.of(idx, inputRel.getRowType()))
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

                //Add timestamp as last project
                TimestampHolder.Derived outputTimestamp = candidate.withIndex(targetLength - 1).fixAsTimestamp();
                projects.add(timestampRef);
                projectNames.add(null);

                relB.project(projects, projectNames);
                ContinuousIndexMap pk = ContinuousIndexMap.identity(groupByIdx.size(), targetLength);
                ContinuousIndexMap select = ContinuousIndexMap.identity(targetLength - 1, targetLength);
                return setRelHolder(RelMeta.build(relB.build(), TableType.TEMPORAL_STATE, pk,
                        outputTimestamp, select, nowInput.materialize).build());
            }
        } else {
            //Standard aggregation produces a state table
            Preconditions.checkArgument(input.nowFilter.isEmpty(),"State table cannot have now-filter since there is no timestamp");
            RelBuilder relB = relBuilderFactory.get();
            relB.push(input.relNode);
            relB.aggregate(relB.groupKey(Ints.toArray(groupByIdx)), aggregateCalls);
            //since there is no timestamp, we cannot propagate a sliding window
            //if (isSlidingAggregate) new TumbleAggregationHint(TumbleAggregationHint.Type.SLIDING).addTo(relB);
            ContinuousIndexMap pk = ContinuousIndexMap.identity(groupByIdx.size(), targetLength);
            ContinuousIndexMap select = ContinuousIndexMap.identity(targetLength, targetLength);
            return setRelHolder(RelMeta.build(relB.build(), TableType.STATE, pk,
                    TimestampHolder.Derived.NONE, select, input.materialize).build());
        }
    }

    private Triple<ContinuousIndexMap, ContinuousIndexMap, TimestampHolder.Derived> addTimestampAggregate(
            RelBuilder relBuilder, List<Integer> groupByIdx, TimestampHolder.Derived.Candidate candidate,
            List<AggregateCall> aggregateCalls) {
        int targetLength = groupByIdx.size() + aggregateCalls.size();
        List<Integer> groupByIdxTimestamp = new ArrayList<>(groupByIdx);
        boolean addedTimestamp = !groupByIdxTimestamp.contains(candidate.getIndex());
        if (addedTimestamp) {
            groupByIdxTimestamp.add(candidate.getIndex());
            targetLength++;
        }
        Collections.sort(groupByIdxTimestamp);
        int newTimestampIdx = groupByIdxTimestamp.indexOf(candidate.getIndex());
        TimestampHolder.Derived timestamp = candidate.withIndex(newTimestampIdx).fixAsTimestamp();

        relBuilder.aggregate(relBuilder.groupKey(Ints.toArray(groupByIdxTimestamp)),aggregateCalls);
        List<Integer> pkIndexes = new ArrayList<>(ContiguousSet.closedOpen(0,groupByIdxTimestamp.size()).asList());
        if (addedTimestamp) pkIndexes.remove(newTimestampIdx);
        ContinuousIndexMap pk = ContinuousIndexMap.of(pkIndexes,targetLength);
        ContinuousIndexMap select = ContinuousIndexMap.of(SqrlRexUtil.combineIndexes(pkIndexes,
                ContiguousSet.closedOpen(groupByIdxTimestamp.size(),targetLength)),targetLength);
        return Triple.of(pk,select,timestamp);
    }

    @Override
    public RelNode visit(LogicalSort logicalSort) {
        Preconditions.checkArgument(logicalSort.offset == null, "OFFSET not yet supported");
        RelMeta input = getRelHolder(logicalSort.getInput().accept(this));

        Optional<Integer> limit = getLimit(logicalSort.fetch);
        if (limit.isPresent()) {
            //Need to inline topN
            input = input.inlineTopN(makeRelBuilder());
        }

        //Map the collation fields
        RelCollation collation = logicalSort.getCollation();
        ContinuousIndexMap indexMap = input.select;
        RelCollation newCollation = SqrlRexUtil.mapCollation(collation, indexMap);

        RelMeta result;
        if (limit.isPresent()) {
            result = input.copy().topN(new TopNConstraint(List.of(),false,newCollation,limit)).build();
        } else {
            //We can just replace any old order that might be present
            result = input.copy().sort(new SortOrder(newCollation)).build();
        }
        return setRelHolder(result);
    }

    public Optional<Integer> getLimit(RexNode limit) {
        if (limit == null) return Optional.empty();
        Preconditions.checkArgument(limit instanceof RexLiteral);
        return Optional.of(((RexLiteral)limit).getValueAs(Integer.class));
    }




}
