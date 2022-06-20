package ai.datasqrl.plan.local;

import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.io.sources.stats.RelationStats;
import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import ai.datasqrl.parse.tree.ComparisonExpression;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.Join;
import ai.datasqrl.parse.tree.JoinOn;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.plan.calcite.CalciteEnvironment;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedColumn;
import ai.datasqrl.plan.local.transpiler.nodes.relation.JoinNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.RelationNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.TableNodeNorm;
import ai.datasqrl.schema.*;
import ai.datasqrl.schema.constraint.NotNull;
import ai.datasqrl.schema.input.FlexibleTableConverter;
import ai.datasqrl.schema.input.RelationType;
import ai.datasqrl.schema.type.ArrayType;
import ai.datasqrl.schema.type.Type;
import ai.datasqrl.schema.type.basic.BasicType;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static ai.datasqrl.parse.util.SqrlNodeUtil.and;

public class BundleTableFactory {

    private final AtomicInteger tableIdCounter = new AtomicInteger(0);
    private final Name parentRelationshipName = ReservedName.PARENT;

    public BundleTableFactory(CalciteEnvironment calciteEnvironment) {
    }

    public Table importTable(ImportManager.SourceTableImport impTbl,
                             Optional<Name> tableAlias) {
        ImportVisitor visitor = new ImportVisitor();
        FlexibleTableConverter converter = new FlexibleTableConverter(impTbl.getSchema(), tableAlias);
        converter.apply(visitor);
        TableBuilder tblBuilder = visitor.lastCreatedTable;
        assert tblBuilder != null;
        Column timestamp = tblBuilder.getFields().stream().filter(f -> f.getName().equals(ReservedName.INGEST_TIME))
                .map(f->(Column)f).findFirst().get();
        return createImportTableHierarchy(tblBuilder, timestamp, impTbl.getTable().getStatistics());
    }

    private Table createImportTableHierarchy(TableBuilder tblBuilder, Column timestamp,
                                             SourceTableStatistics statistics) {
        NamePath tblPath = tblBuilder.getPath();
        RelationStats stats = statistics.getRelationStats(tblPath.subList(1,tblPath.getLength()));
        Table table = tblBuilder.createTable(Table.Type.STREAM, timestamp, TableStatistic.from(stats));
        //Recurse through children and add parent-child relationships
        for (Pair<TableBuilder,Relationship.Multiplicity> child : tblBuilder.children) {
            TableBuilder childBuilder = child.getKey();
            //Add parent timestamp as internal column
            Column childTimestamp = childBuilder.addColumn(ReservedName.TIMESTAMP,
                false, false, true, false);
            Table childTbl = createImportTableHierarchy(childBuilder, childTimestamp, statistics);
            Name childName = childBuilder.getPath().getLast();
            Optional<Relationship> parentRel = createParentRelationship(childTbl, table);
            parentRel.map(rel -> childTbl.getFields().add(rel));
            Relationship childRel = createChildRelationship(childName, childTbl, table, child.getValue());
            table.getFields().add(childRel);
        }
        return table;
    }

    public Optional<Relationship> createParentRelationship(Table childTable, Table parentTable) {
        //Avoid overwriting an existing "parent" column on the child
        if (childTable.getField(parentRelationshipName).isEmpty()) {
            Relationship parentRel = new Relationship(parentRelationshipName,
                    childTable, parentTable, Relationship.JoinType.PARENT, Relationship.Multiplicity.ONE,
                    createParentChildRelation(Join.Type.INNER, parentTable.getPrimaryKeys(), childTable, parentTable));
            return Optional.of(parentRel);
        }
        return Optional.empty();
    }


    public Relationship createChildRelationship(Name childName, Table childTable, Table parentTable,
                                              Relationship.Multiplicity multiplicity) {
        Relationship childRel = new Relationship(childName,
                parentTable, childTable, Relationship.JoinType.CHILD, multiplicity,
                createParentChildRelation(Join.Type.INNER, parentTable.getPrimaryKeys(), parentTable, childTable),
                Optional.empty(), Optional.empty());
        return childRel;
    }


    private RelationNorm createParentChildRelation(Join.Type type, List<Column> keys, Table from, Table to) {
        TableNodeNorm fromNorm = TableNodeNorm.of(from);
        TableNodeNorm toNorm = TableNodeNorm.of(to);

        List<Expression> criteria = keys.stream()
                .map(column ->
                        new ComparisonExpression(ComparisonExpression.Operator.EQUAL,
                                ResolvedColumn.of(fromNorm, column),
                                ResolvedColumn.of(toNorm, column)))
                .collect(Collectors.toList());

        return new JoinNorm(Optional.empty(), type, fromNorm, toNorm, JoinOn.on(and(criteria)));
    }

    private class ImportVisitor implements FlexibleTableConverter.Visitor<Type> {

        private final Deque<TableBuilder> stack = new ArrayDeque<>();
        private TableBuilder lastCreatedTable = null;


        @Override
        public void beginTable(Name name, NamePath namePath, boolean isNested, boolean isSingleton, boolean hasSourceTimestamp) {
            TableBuilder tblBuilder = new TableBuilder(namePath.concat(name));
            //Add primary keys
            if (isNested) {
                //Add parent primary keys
                tblBuilder.addParentPrimaryKeys(stack.getFirst());
                //Add denormalized timestap placeholder
                if (!isSingleton) {
                    tblBuilder.addColumn(ReservedName.ARRAY_IDX,true,
                            false, true, true);
                }
            } else {
                tblBuilder.addColumn(ReservedName.UUID,true,
                        false, true, true);
                tblBuilder.addColumn(ReservedName.INGEST_TIME, false,
                        false, true, true);
                if (hasSourceTimestamp) {
                    tblBuilder.addColumn(ReservedName.SOURCE_TIME, false,
                            false, true, true);
                }
            }
            stack.addFirst(tblBuilder);
        }

        @Override
        public Optional<Type> endTable(Name name, NamePath namePath, boolean isNested, boolean isSingleton) {
            lastCreatedTable = stack.removeFirst();
            return Optional.of(RelationType.EMPTY);
        }

        @Override
        public void addField(Name name, Type type, boolean notnull) {
            if (isRelationType(type)) {
                //It's a relationship
                Relationship.Multiplicity multi = Relationship.Multiplicity.ZERO_ONE;
                if (type instanceof ArrayType) multi = Relationship.Multiplicity.MANY;
                else if (notnull) multi = Relationship.Multiplicity.ONE;
                stack.getFirst().addChild(lastCreatedTable,multi);
                lastCreatedTable = null;
            } else {
                //It's a column
                stack.getFirst().addColumn(name, false,
                        false, notnull, true);
            }
        }


        private boolean isRelationType(Type type) {
            if (type instanceof RelationType) return true;
            if (type instanceof ArrayType) {
                return isRelationType(((ArrayType)type).getSubType());
            }
            return false;
        }

        @Override
        public Type convertBasicType(BasicType type) {
            return type;
        }

        @Override
        public Type wrapArray(Type type, boolean notnull) {
            return new ArrayType(type); //Nullability is ignored
        }


    }

    public TableBuilder build(NamePath tableName) {
        return new TableBuilder(tableName);
    }

    public class TableBuilder extends AbstractTable {

        private final List<Pair<TableBuilder, Relationship.Multiplicity>> children = new ArrayList<>();
        private int columnCounter = 0;

        private TableBuilder(NamePath namePath) {
            super(tableIdCounter.incrementAndGet(), namePath, new ShadowingContainer<>());
        }

        private void addChild(TableBuilder table, Relationship.Multiplicity multi) {
            children.add(Pair.of(table,multi));
        }

        public Column addColumn(Name name, boolean isPrimaryKey, boolean isParentPrimaryKey,
                       boolean notnull, boolean isVisible) {
            int version = getNextColumnVersion(name);
            Column col = new Column(name, version, columnCounter++,
                    isPrimaryKey, isParentPrimaryKey,
                    notnull? List.of(NotNull.INSTANCE) : List.of(), isVisible);
            fields.add(col);
            return col;
        }

        public void addParentPrimaryKeys(AbstractTable parent) {
            for (Column ppk : parent.getPrimaryKeys()) {
                addColumn(ppk.getName(), true, true, true, false);
            }
        }

        public Table createTable(Table.Type type, Column timestamp, TableStatistic statistic) {
            return new Table(uniqueId, path, type, fields, timestamp, statistic);
        }

    }

}
