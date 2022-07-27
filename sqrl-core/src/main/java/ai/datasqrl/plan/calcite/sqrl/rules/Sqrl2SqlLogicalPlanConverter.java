package ai.datasqrl.plan.calcite.sqrl.rules;

import ai.datasqrl.plan.calcite.sqrl.hints.ExplicitInnerJoinTypeHint;
import ai.datasqrl.plan.calcite.sqrl.hints.NumColumnsHint;
import ai.datasqrl.plan.calcite.sqrl.hints.SqrlHint;
import ai.datasqrl.plan.calcite.sqrl.table.*;
import ai.datasqrl.plan.calcite.util.CalciteUtil;
import ai.datasqrl.plan.calcite.util.ContinuousIndexMap;
import ai.datasqrl.plan.calcite.util.IndexMap;
import ai.datasqrl.plan.calcite.util.SqrlRexUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import lombok.Value;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Value
public class Sqrl2SqlLogicalPlanConverter extends AbstractSqrlRelShuttle<Sqrl2SqlLogicalPlanConverter.ProcessedRel> {

    public final Supplier<RelBuilder> relBuilderFactory;
    public final SqrlRexUtil rexUtil;

    @Value
    public static class ProcessedRel implements RelHolder {

        RelNode relNode;
        QuerySqrlTable.Type type;
        ContinuousIndexMap primaryKey;
        TimestampHolder.Derived timestamp;
        ContinuousIndexMap indexMap;

        List<JoinTable> joinTables;
        TopNConstraint topN;
    }

    public ProcessedRel putPrimaryKeysUpfront(ProcessedRel input) {
        ContinuousIndexMap indexMap = input.indexMap;
        HashMap<Integer,Integer> remapping = new HashMap<>();
        int index = 0;
        for (int i = 0; i < input.primaryKey.getSourceLength(); i++) {
            remapping.put(input.primaryKey.map(i),index++);
        }
        for (int i = 0; i < input.indexMap.getTargetLength(); i++) {
            if (!remapping.containsKey(i)) {
                remapping.put(i,index++);
            }
        }
        Preconditions.checkArgument(index==indexMap.getTargetLength() && remapping.size() == index);
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
                input.timestamp.remapIndexes(remap), input.indexMap.remap(remap), null, input.topN.remap(remap));

    }

    /**
     * Called to inline the TopNConstraint on top of the input relation
     * @return
     */
    private ProcessedRel inlineTopN(ProcessedRel input) {
        Preconditions.checkArgument(input.topN.isEmpty(),"Not yet implemented");
        //Redo the pattern extracted in extractTopNConstraint
        return input;
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
        //The base scan tables for all SQRL queries are VirtualSqrlTable
        VirtualSqrlTable vtable = tableScan.getTable().unwrap(VirtualSqrlTable.class);
        Preconditions.checkArgument(vtable != null);

        //Shred the virtual table all the way to root:
        //First, we prepare all the data structures
        ContinuousIndexMap.Builder indexMap = ContinuousIndexMap.builder(vtable.getNumColumns());
        List<JoinTable> joinTables = new ArrayList<>();
        ContinuousIndexMap.Builder primaryKey = ContinuousIndexMap.builder(vtable.getNumPrimaryKeys());
        //Now, we shred
        RelNode relNode = shredTable(vtable, primaryKey, indexMap, joinTables, true).build();
        //Finally, we assemble the result
        VirtualSqrlTable.Root root = vtable.getRoot();
        QuerySqrlTable queryTable = root.getBase();
        int mapToLength = relNode.getRowType().getFieldCount();
        ProcessedRel result = new ProcessedRel(relNode, queryTable.getType(),
                primaryKey.build(mapToLength),
                new TimestampHolder.Derived(queryTable.getTimestamp()),
                indexMap.build(mapToLength), joinTables, queryTable.getTopN());
        return setRelHolder(result);
    }

    private RelBuilder shredTable(VirtualSqrlTable vtable, ContinuousIndexMap.Builder primaryKey,
                                  ContinuousIndexMap.Builder indexMap, List<JoinTable> joinTables,
                                  boolean isLeaf) {
        Preconditions.checkArgument(joinTables.isEmpty());
        return shredTable(vtable, primaryKey, indexMap, joinTables, null, isLeaf);
    }

    private RelBuilder shredTable(VirtualSqrlTable vtable, ContinuousIndexMap.Builder primaryKey,
                                  List<JoinTable> joinTables, Pair<JoinTable,RelBuilder> startingBase) {
        Preconditions.checkArgument(joinTables.isEmpty());
        return shredTable(vtable, primaryKey, null, joinTables, startingBase, false);
    }

    private RelBuilder shredTable(VirtualSqrlTable vtable, ContinuousIndexMap.Builder primaryKey,
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
            VirtualSqrlTable.Root root = (VirtualSqrlTable.Root) vtable;
            offset = 0;
            builder = relBuilderFactory.get();
            builder.scan(root.getBase().getNameId());
            //Since inlined columns can be added to the base table, we need to project to the current size
            //Add as hint since identity projections are filtered out by builder
            builder.hints(new NumColumnsHint(root.getNumQueryColumns()).getHint());
            joinTable = JoinTable.ofRoot(root);
            columns2Add = vtable.getAddedColumns().stream()
                    .filter(Predicate.not(AddedColumn::isInlined))
                    .collect(Collectors.toList());
        } else {
            VirtualSqrlTable.Child child = (VirtualSqrlTable.Child) vtable;
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
        if (input.topN.hasLimit()) input = inlineTopN(input); //Filtering doesn't preserve limits
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
                timestamp,input.indexMap,input.joinTables, input.topN));
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
            //If it's a trivial project, we remove it and replace only update the indexMap
            return setRelHolder(new ProcessedRel(rawInput.relNode,rawInput.type,rawInput.primaryKey, rawInput.timestamp,
                    trivialMap, rawInput.joinTables, rawInput.topN));
        }
        ProcessedRel input = inlineTopN(rawInput);
        Preconditions.checkArgument(input.topN.isEmpty());
        List<RexNode> updatedProjects = new ArrayList<>();
        Multimap<Integer,Integer> mappedProjects = HashMultimap.create();
        for (Ord<RexNode> exp : Ord.<RexNode>zip(logicalProject.getProjects())) {
            RexNode mapRex = SqrlRexUtil.mapIndexes(exp.e,input.indexMap);
            updatedProjects.add(exp.i,mapRex);
            if (mapRex instanceof RexInputRef) {
                int index = (((RexInputRef) mapRex)).getIndex();
                mappedProjects.put(index,exp.i);
            }
        }
        //Make sure we pull the primary keys and timestamp (candidates) through (i.e. append those to the projects
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
        List<TimestampHolder.Candidate> timeCandidates = new ArrayList<>();
        for (TimestampHolder.Candidate candidate : input.timestamp.getCandidates()) {
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

        RelBuilder relB = relBuilderFactory.get();
        relB.push(input.relNode);
        relB.project(updatedProjects);
        relB.hints(logicalProject.getHints());
        RelNode newProject = relB.build();
        int fieldCount = updatedProjects.size();
        return setRelHolder(new ProcessedRel(newProject,input.type,primaryKey.build(fieldCount),
                timestamp, ContinuousIndexMap.identity(logicalProject.getProjects().size(),fieldCount),null,
                input.topN));
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

        Preconditions.checkArgument(logicalJoin.getJoinType() == JoinRelType.INNER, "Unsupported join type: %s", logicalJoin);
        Optional<ExplicitInnerJoinTypeHint> joinTypeHint = SqrlHint.fromRel(logicalJoin, ExplicitInnerJoinTypeHint.CONSTRUCTOR);

        ContinuousIndexMap joinedIndexMap = leftInput.indexMap.join(rightInput.indexMap);
        RexNode condition = SqrlRexUtil.mapIndexes(logicalJoin.getCondition(),joinedIndexMap);

        //Identify if this is an identical self-join for a nested tree
        if (leftInput.joinTables!=null && rightInput.joinTables!=null && leftInput.topN.isEmpty() && rightInput.topN.isEmpty()) {
            SqrlRexUtil.EqualityComparisonDecomposition eqDecomp = rexUtil.decomposeEqualityComparison(condition);
            if (eqDecomp.getRemainingPredicates().isEmpty()) {
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
                    ContinuousIndexMap remapedRight = rightInput.indexMap.remap(relNode.getRowType().getFieldCount(),
                            index -> {
                                JoinTable jt = JoinTable.find(rightInput.joinTables,index).get();
                                return right2left.get(jt).getGlobalIndex(jt.getLocalIndex(index));
                            });
                    ContinuousIndexMap indexMap = leftInput.indexMap.append(remapedRight);
                    return setRelHolder(new ProcessedRel(relNode, leftInput.type,
                            newPk, leftInput.timestamp, indexMap, joinTables, TopNConstraint.EMPTY));

                }
            }
        }

        return null;
    }

    @Override
    public RelNode visit(LogicalUnion logicalUnion) {
        throw new UnsupportedOperationException("Not yet supported");
    }

    @Override
    public RelNode visit(LogicalAggregate input) {
        return null;
    }

    @Override
    public RelNode visit(LogicalSort logicalSort) {
        Preconditions.checkArgument(logicalSort.offset == null && logicalSort.fetch == null, "OFFSET not yet supported");
        ProcessedRel input = getRelHolder(logicalSort.getInput().accept(this));
        Preconditions.checkArgument(!input.topN.hasPartition(),"Sorting on top of a partitioned relation is invalid");
        if (input.topN.isDistinct() || input.topN.hasLimit()) {
            //Need to inline before we can sort on top
            input = inlineTopN(input);
        } //else there is only a sort which we replace by this sort if present

        RelCollation collation = logicalSort.getCollation();
        //Map the collation fields
        ContinuousIndexMap indexMap = input.indexMap;
        RelCollation newCollation = RelCollations.of(collation.getFieldCollations().stream()
                .map(fc -> fc.withFieldIndex(indexMap.map(fc.getFieldIndex()))).collect(Collectors.toList()));
        if (newCollation.getFieldCollations().isEmpty()) newCollation = input.topN.getCollation();

        TopNConstraint topN = new TopNConstraint(newCollation, List.of(), getLimit(logicalSort.fetch), false);

        return setRelHolder(new ProcessedRel(input.relNode,input.type,input.primaryKey,
                input.timestamp,input.indexMap,input.joinTables, topN));
    }

    public Optional<Integer> getLimit(RexNode limit) {
        Preconditions.checkArgument(limit instanceof RexLiteral);
        return Optional.of(((RexLiteral)limit).getValueAs(Integer.class));
    }




}
