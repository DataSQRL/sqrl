package ai.datasqrl.plan.calcite.sqrl.rules;

import ai.datasqrl.plan.calcite.sqrl.hints.ExplicitInnerJoinTypeHint;
import ai.datasqrl.plan.calcite.sqrl.hints.NumColumnsHint;
import ai.datasqrl.plan.calcite.sqrl.hints.SqrlHint;
import ai.datasqrl.plan.calcite.sqrl.table.AddedColumn;
import ai.datasqrl.plan.calcite.sqrl.table.QuerySqrlTable;
import ai.datasqrl.plan.calcite.sqrl.table.TimestampHolder;
import ai.datasqrl.plan.calcite.sqrl.table.VirtualSqrlTable;
import ai.datasqrl.plan.calcite.util.CalciteUtil;
import ai.datasqrl.plan.calcite.util.ContinuousIndexMap;
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
public class Sqrl2SqlLogicalPlanConverter extends AbstractSqrlRelShuttle<Sqrl2SqlLogicalPlanConverter.Metadata> {

    public final Supplier<RelBuilder> relBuilderFactory;
    public final SqrlRexUtil rexUtil;

    @Value
    public static class Metadata implements RelHolder {

        RelNode relNode;
        QuerySqrlTable.Type type;
        ContinuousIndexMap primaryKey;
        TimestampHolder timestamp;
        ContinuousIndexMap indexMap;

        List<JoinTable> joinTables;
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
        Metadata result = new Metadata(relNode, queryTable.getType(),
                primaryKey.build(mapToLength),
                queryTable.getTimestamp(),
                indexMap.build(mapToLength), joinTables);
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
        Metadata input = getRelHolder(logicalFilter.getInput().accept(this));
        RexNode condition = logicalFilter.getCondition();
        condition = SqrlRexUtil.mapIndexes(condition,input.indexMap);
        //Check if it has a now() predicate and pull out or throw an exception if malformed
        LogicalFilter filter;
        TimestampHolder timestamp = input.timestamp;
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
        return setRelHolder(new Metadata(filter,input.type,input.primaryKey,
                timestamp,input.indexMap,input.joinTables));
    }

    private static Optional<Integer> getRecencyFilterTimestampIndex(RexNode condition) {
        //TODO: implement, reuse Flink code to determine interval
        return Optional.empty();
    }

    @Override
    public RelNode visit(LogicalProject logicalProject) {
        Metadata input = getRelHolder(logicalProject.getInput().accept(this));
        //TODO: Detect if this is a distinct/top-n pattern and pull out

        ContinuousIndexMap trivialMap = getTrivialMapping(logicalProject, input.indexMap);
        if (trivialMap!=null) {
            //If it's a trivial project, we remove it and replace only update the indexMap
            return setRelHolder(new Metadata(input.relNode,input.type,input.primaryKey, input.timestamp,
                    trivialMap, input.joinTables));
        }
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
        TimestampHolder timestamp = new TimestampHolder(input.timestamp,timeCandidates);

        RelBuilder relB = relBuilderFactory.get();
        relB.push(input.relNode);
        relB.project(updatedProjects);
        relB.hints(logicalProject.getHints());
        RelNode newProject = relB.build();
        int fieldCount = updatedProjects.size();
        return setRelHolder(new Metadata(newProject,input.type,primaryKey.build(fieldCount),
                timestamp, ContinuousIndexMap.identity(logicalProject.getProjects().size(),fieldCount),null));
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
        Metadata leftInput = getRelHolder(logicalJoin.getLeft().accept(this));
        Metadata rightInput = getRelHolder(logicalJoin.getRight().accept(this));

        Preconditions.checkArgument(logicalJoin.getJoinType() == JoinRelType.INNER, "Unsupported join type: %s", logicalJoin);
        Optional<ExplicitInnerJoinTypeHint> joinTypeHint = SqrlHint.fromRel(logicalJoin, ExplicitInnerJoinTypeHint.CONSTRUCTOR);

        ContinuousIndexMap joinedIndexMap = leftInput.indexMap.join(rightInput.indexMap);
        RexNode condition = SqrlRexUtil.mapIndexes(logicalJoin.getCondition(),joinedIndexMap);

        //Identify if this is an identical self-join for a nested tree
        if (leftInput.joinTables!=null && rightInput.joinTables!=null) {
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
                    return setRelHolder(new Metadata(relNode, leftInput.type,
                            newPk, leftInput.timestamp, indexMap, joinTables));

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
    public RelNode visit(LogicalAggregate logicalAggregate) {
        return null;
    }

    @Override
    public RelNode visit(LogicalSort logicalSort) {
        //TODO: Extract sort when top level and convert to window otherwise
        Preconditions.checkArgument(logicalSort.offset == null && logicalSort.fetch == null, "OFFSET and LIMIT not yet supported");
        Metadata input = getRelHolder(logicalSort.getInput().accept(this));
        RelCollation collation = logicalSort.getCollation();
        //Map the collation fields
        ContinuousIndexMap indexMap = input.indexMap;
        RelCollation newCollation = RelCollations.of(collation.getFieldCollations().stream()
                .map(fc -> fc.withFieldIndex(indexMap.map(fc.getFieldIndex()))).collect(Collectors.toList()));
        RelNode newSort = logicalSort.copy(logicalSort.getTraitSet(),input.relNode,newCollation, logicalSort.offset, logicalSort.fetch);
        return setRelHolder(new Metadata(newSort,input.type,input.primaryKey,
                input.timestamp,input.indexMap,input.joinTables));
    }




}
