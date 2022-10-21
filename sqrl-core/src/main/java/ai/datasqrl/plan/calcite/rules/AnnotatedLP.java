package ai.datasqrl.plan.calcite.rules;

import ai.datasqrl.physical.EngineCapability;
import ai.datasqrl.plan.calcite.table.*;
import ai.datasqrl.plan.calcite.util.CalciteUtil;
import ai.datasqrl.plan.calcite.util.ContinuousIndexMap;
import ai.datasqrl.plan.calcite.util.IndexMap;
import ai.datasqrl.plan.calcite.util.SqrlRexUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ContiguousSet;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import java.util.*;

@Value
@AllArgsConstructor
@Builder
public class AnnotatedLP implements RelHolder {

    @NonNull
    public RelNode relNode;
    public TableType type;
    @NonNull
    public ContinuousIndexMap primaryKey;
    @NonNull
    public TimestampHolder.Derived timestamp;
    @NonNull
    public ContinuousIndexMap select;
    @NonNull
    public ExecutionAnalysis exec;

    @Builder.Default
    public List<JoinTable> joinTables = null;
    @Builder.Default
    public Optional<Integer> numRootPks = Optional.empty();

    @Builder.Default
    @NonNull
    public NowFilter nowFilter = NowFilter.EMPTY; //Applies before topN
    @Builder.Default
    @NonNull
    public TopNConstraint topN = TopNConstraint.EMPTY; //Applies before sort
    @Builder.Default
    @NonNull
    public SortOrder sort = SortOrder.EMPTY;

    public static AnnotatedLPBuilder build(RelNode relNode, TableType type, ContinuousIndexMap primaryKey,
                                       TimestampHolder.Derived timestamp, ContinuousIndexMap select,
                                           ExecutionAnalysis exec) {
        return AnnotatedLP.builder().relNode(relNode).type(type).primaryKey(primaryKey).timestamp(timestamp)
                .select(select).exec(exec);
    }

    public AnnotatedLPBuilder copy() {
        AnnotatedLPBuilder builder = AnnotatedLP.builder();
        builder.relNode(relNode);
        builder.type(type);
        builder.primaryKey(primaryKey);
        builder.timestamp(timestamp);
        builder.select(select);
        builder.joinTables(joinTables);
        builder.numRootPks(numRootPks);
        builder.exec(exec);
        builder.nowFilter(nowFilter);
        builder.topN(topN);
        builder.sort(sort);
        return builder;
    }

    public int getFieldLength() {
        return relNode.getRowType().getFieldCount();
    }

    /**
     * Called to inline the TopNConstraint on top of the input relation.
     * This will inline a nowFilter if present
     *
     * @return
     */
    public AnnotatedLP inlineTopN(RelBuilder relBuilder) {
        if (topN.isEmpty()) return this;
        if (!nowFilter.isEmpty()) return inlineNowFilter(relBuilder).inlineTopN(relBuilder);

        Preconditions.checkArgument(nowFilter.isEmpty());

        relBuilder.push(relNode);

        ExecutionAnalysis newExec = exec;
        SortOrder newSort = sort;
        if (!topN.isDistinct() && (!topN.hasPartition() || !topN.hasLimit())) {
            assert topN.hasCollation();
            RelCollation collation = topN.getCollation();
            if (topN.hasLimit()) { //It's not partitioned, so straight forward order and limit
                relBuilder.sort(collation);
                relBuilder.limit(0, topN.getLimit());
                newExec = exec.require(EngineCapability.GLOBAL_SORT);
            } else { //Lift up sort and prepend partition (if any)
                newSort = newSort.ifEmpty(SortOrder.of(topN.getPartition(), collation));
            }
            return AnnotatedLP.build(relBuilder.build(), type, primaryKey, timestamp, select, newExec)
                    .sort(newSort).build();
        } else { //distinct or (hasPartition and hasLimit)
            final RelDataType inputType = relBuilder.peek().getRowType();
            RexBuilder rexBuilder = relBuilder.getRexBuilder();

            List<Integer> projectIdx = ContiguousSet.closedOpen(0, inputType.getFieldCount()).asList();
            List<Integer> partitionIdx = topN.getPartition();
            if (topN.isDistinct() && !topN.hasPartition()) { //Partition by all distinct columns
                partitionIdx = primaryKey.targetsAsList();
            }

            int rowFunctionColumns = 1;
            if (topN.isDistinct() && topN.hasPartition()) {
                rowFunctionColumns += topN.hasLimit() ? 2 : 1;
            }
            int projectLength = projectIdx.size() + rowFunctionColumns;

            List<RexNode> partitionKeys = new ArrayList<>(partitionIdx.size());
            List<RexNode> projects = new ArrayList<>(projectLength);
            List<String> projectNames = new ArrayList<>(projectLength);
            for (Integer idx : projectIdx) {
                RexInputRef ref = RexInputRef.of(idx, inputType);
                projects.add(ref);
                projectNames.add(null);
                if (partitionIdx.contains(idx)) partitionKeys.add(ref);
            }
            assert projects.size() == projectIdx.size() && partitionKeys.size() == partitionIdx.size();

            List<RexFieldCollation> fieldCollations = new ArrayList<>();
            fieldCollations.addAll(SqrlRexUtil.translateCollation(topN.getCollation(), inputType));
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
            projects.add(rexUtil.createRowFunction(SqlStdOperatorTable.ROW_NUMBER, partitionKeys, fieldCollations));
            projectNames.add(null);
            int rowNumberIdx = projectIdx.size(), rankIdx = rowNumberIdx + 1, denserankIdx = rowNumberIdx + 2;
            if (topN.isDistinct() && topN.hasPartition()) {
                //Add rank and dense_rank if we have a limit
                projects.add(rexUtil.createRowFunction(SqlStdOperatorTable.RANK, partitionKeys, fieldCollations));
                projectNames.add(null);
                if (topN.hasLimit()) {
                    projects.add(rexUtil.createRowFunction(SqlStdOperatorTable.DENSE_RANK, partitionKeys, fieldCollations));
                    projectNames.add(null);
                }
                newExec = newExec.require(EngineCapability.MULTI_RANK);
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
            if (topN.hasPartition() && (!topN.hasLimit() || topN.getLimit() > 1)) {
                //Add sort on top
                SortOrder sortByRowNum = SortOrder.of(topN.getPartition(), RelCollations.of(new RelFieldCollation(rowNumberIdx,
                        RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST)));
                newSort = newSort.ifEmpty(sortByRowNum);
            }
            ContinuousIndexMap newPk = primaryKey.remap(IndexMap.IDENTITY);
            ContinuousIndexMap newSelect = select.remap(IndexMap.IDENTITY);
            return AnnotatedLP.build(relBuilder.build(), type, newPk, timestamp, newSelect, newExec)
                    .sort(newSort).build();
        }
    }

    public AnnotatedLP inlineNowFilter(RelBuilder relB) {
        if (nowFilter.isEmpty()) return this;
        nowFilter.addFilterTo(relB.push(relNode));
        return copy().relNode(relB.build())
                .exec(exec.require(EngineCapability.NOW))
                .nowFilter(NowFilter.EMPTY).build();

    }

    public AnnotatedLP inlineSort(RelBuilder relB) {
        if (sort.isEmpty()) return this;
        //Need to inline now-filter and topN first
        if (!nowFilter.isEmpty()) return inlineNowFilter(relB).inlineSort(relB);
        if (!topN.isEmpty()) return inlineTopN(relB).inlineSort(relB);
        sort.addTo(relB.push(relNode));
        return copy().relNode(relB.build())
                .exec(exec.require(EngineCapability.GLOBAL_SORT))
                .sort(SortOrder.EMPTY).build();
    }

    public AnnotatedLP inlineAllPullups(RelBuilder relB) {
        return inlineNowFilter(relB).inlineTopN(relB).inlineSort(relB);
    }

    public boolean hasPullups() {
        return !topN.isEmpty() || !nowFilter.isEmpty() || !sort.isEmpty();
    }

    public PullupOperator.Container getPullups() {
        return new PullupOperator.Container(nowFilter, topN, sort);
    }

    /**
     * Moves the primary key columns to the front and adds projection to only return
     * columns that the user selected, are part of the primary key, a timestamp candidate, or
     * part of the sort order.
     *
     * Inlines deduplication in case of nested data.
     *
     * @return
     */
    public AnnotatedLP postProcess(RelBuilder relBuilder, List<String> fieldNames) {
        Preconditions.checkArgument(fieldNames.size()==select.getSourceLength());
        List<RelDataTypeField> fields = relNode.getRowType().getFieldList();
        AnnotatedLP input = this;
        if (!topN.isEmpty() && //If any selected field is nested we have to inline topN
                select.targetsAsList().stream().map(fields::get).map(RelDataTypeField::getType).anyMatch(CalciteUtil::isNestedTable)) {
            input = input.inlineTopN(relBuilder);
        }
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

        Preconditions.checkArgument(index<=input.getFieldLength() && remapping.size() == index);
        IndexMap remap = IndexMap.of(remapping);
        ContinuousIndexMap updatedSelect = input.select.remap(remap);
        List<RexNode> projects = new ArrayList<>(input.getFieldLength());
        RelDataType rowType = input.relNode.getRowType();
        remapping.entrySet().stream().map(e -> new IndexMap.Pair(e.getKey(),e.getValue()))
                .sorted((a, b)-> Integer.compare(a.getTarget(),b.getTarget()))
                .forEach(p -> {
                    projects.add(p.getTarget(),RexInputRef.of(p.getSource(), rowType));
                });
        List<String> updatedFieldNames = Arrays.asList(new String[projects.size()]);
        for (int i = 0; i < fieldNames.size(); i++) {
            updatedFieldNames.set(updatedSelect.map(i),fieldNames.get(i));
        }
        relBuilder.push(input.relNode);
        relBuilder.project(projects, updatedFieldNames);
        RelNode relNode = relBuilder.build();

        return new AnnotatedLP(relNode,input.type,input.primaryKey.remap(remap),
                input.timestamp.remapIndexes(remap), updatedSelect, input.exec, null, input.numRootPks,
                input.nowFilter.remap(remap), input.topN.remap(remap), input.sort.remap(remap));
    }

    public AnnotatedLP postProcess(RelBuilder relBuilder) {
        return postProcess(relBuilder,Collections.nCopies(select.getSourceLength(),null));
    }

    public double estimateRowCount() {
        final RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();
        return mq.getRowCount(relNode);
    }


}
