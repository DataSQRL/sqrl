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
import ai.datasqrl.plan.calcite.CalcitePlanner;
import ai.datasqrl.plan.calcite.SqrlType2Calcite;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedColumn;
import ai.datasqrl.plan.local.transpiler.nodes.relation.JoinNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.RelationNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.TableNodeNorm;
import ai.datasqrl.plan.nodes.SqrlCalciteTable;
import ai.datasqrl.plan.nodes.SqrlRelBuilder;
import ai.datasqrl.schema.*;
import ai.datasqrl.schema.constraint.NotNull;
import ai.datasqrl.schema.input.FlexibleTableConverter;
import ai.datasqrl.schema.input.RelationType;
import ai.datasqrl.schema.type.ArrayType;
import ai.datasqrl.schema.type.Type;
import ai.datasqrl.schema.type.basic.BasicType;
import ai.datasqrl.schema.type.basic.DateTimeType;
import ai.datasqrl.schema.type.basic.IntegerType;
import ai.datasqrl.schema.type.basic.UuidType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static ai.datasqrl.parse.util.SqrlNodeUtil.and;

public class BundleTableFactory {

    private final AtomicInteger tableIdCounter = new AtomicInteger(0);
    private final SqrlType2Calcite typeConverter;

    public BundleTableFactory(CalciteEnvironment calciteEnvironment) {
        this.typeConverter = calciteEnvironment.getTypeConverter();
    }

    public Table importTable(CalcitePlanner calcitePlanner, ImportManager.SourceTableImport impTbl,
                             Optional<Name> tableAlias) {
        ImportVisitor visitor = new ImportVisitor();
        FlexibleTableConverter converter = new FlexibleTableConverter(impTbl.getSourceSchema(), tableAlias);
        converter.apply(visitor);
        TableBuilder tblBuilder = visitor.lastCreatedTable;
        assert tblBuilder != null;
        //Identify timestamp column; TODO: additional method argument for explicit timestamp definition
        Column timestamp = tblBuilder.fields.stream().filter(f -> f.getName().equals(ReservedName.INGEST_TIME))
                .map(f->(Column)f).findFirst().get();
        return createImportTableHierarchy(calcitePlanner, tblBuilder,timestamp,
                null, impTbl.getTable().getStatistics());
    }

    private Table createImportTableHierarchy(CalcitePlanner calcitePlanner,
                                             TableBuilder tblBuilder, Column timestamp,
                                             Table parentTbl, SourceTableStatistics statistics) {
        if (parentTbl != null) {
            //Add parent primary keys to child
            tblBuilder.addParentPrimaryKeys(parentTbl);
        }
        NamePath tblPath = tblBuilder.namePath;
        RelationStats stats = statistics.getRelationStats(tblPath.subList(1,tblPath.getLength()));
        SqrlRelBuilder relBuilder = calcitePlanner.createRelBuilder();
        RelNode tableHead = relBuilder.scanStream(tblPath.getLast(),tblBuilder).build();
        Table table = tblBuilder.createTable(Table.Type.STREAM, timestamp, tableHead, TableStatistic.from(stats));
        //Recurse through children and add parent-child relationships
        for (Pair<TableBuilder,Relationship.Multiplicity> child : tblBuilder.children) {
            TableBuilder childBuilder = child.getKey();
            Table childTbl = createImportTableHierarchy(calcitePlanner, childBuilder, null, table, statistics);
            Name childName = childBuilder.namePath.getLast();
            createParentChildRelationship(childName, childTbl, table, child.getValue());
        }
        return table;
    }

    public void createParentChildRelationship(Name childName, Table childTable, Table parentTable,
                                              Relationship.Multiplicity multiplicity) {
        //Built-in relationships
        Relationship parentRel = new Relationship(ReservedName.PARENT,
                childTable, parentTable, Relationship.JoinType.PARENT, Relationship.Multiplicity.ONE,
                createParentChildRelation(Join.Type.INNER, parentTable.getPrimaryKeys(), childTable, parentTable));
        childTable.getFields().add(parentRel);

        Relationship childRel = new Relationship(childName,
                parentTable, childTable, Relationship.JoinType.CHILD, multiplicity,
                createParentChildRelation(Join.Type.INNER, parentTable.getPrimaryKeys(), parentTable, childTable),
                Optional.empty(), Optional.empty());
        parentTable.getFields().add(childRel);
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

    private RelDataType convertType(Type type) {
        return type.accept(typeConverter,null);
    }

    private class ImportVisitor implements FlexibleTableConverter.Visitor<Type> {

        private final Deque<TableBuilder> stack = new ArrayDeque<>();
        private TableBuilder lastCreatedTable = null;


        @Override
        public void beginTable(Name name, NamePath namePath, boolean isNested, boolean isSingleton) {
            //Add primary keys
            stack.addFirst(new TableBuilder(namePath.concat(name)));
        }

        protected void augmentTable(TableBuilder tblBuilder, boolean isNested, boolean isSingleton) {
            if (!isNested) {
                tblBuilder.addColumn(ReservedName.UUID, convertType(UuidType.INSTANCE), true,
                        false, true, false);
                tblBuilder.addColumn(ReservedName.INGEST_TIME, convertType(DateTimeType.INSTANCE), false,
                        false, true, false);
                tblBuilder.addColumn(ReservedName.SOURCE_TIME, convertType(DateTimeType.INSTANCE), false,
                        false, false, false);
            }
            if (isNested && !isSingleton) {
                tblBuilder.addColumn(ReservedName.ARRAY_IDX, convertType(IntegerType.INSTANCE), true,
                        false, true, true);
            }
        }

        @Override
        public Optional<Type> endTable(Name name, NamePath namePath, boolean isNested, boolean isSingleton) {
            lastCreatedTable = stack.removeFirst();
            augmentTable(lastCreatedTable, isNested, isSingleton);
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
                stack.getFirst().addColumn(name, convertType(type), false,
                        false, notnull, false);
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

    public class TableBuilder {

        private final NamePath namePath;
        private final ShadowingContainer<Field> fields = new ShadowingContainer<>();
        private final List<Pair<TableBuilder, Relationship.Multiplicity>> children = new ArrayList<>();
        private int columnCounter = 0;

        private TableBuilder(NamePath namePath) {
            this.namePath = namePath;
        }

        private void addChild(TableBuilder table, Relationship.Multiplicity multi) {
            children.add(Pair.of(table,multi));
        }

        public void addColumn(Name name, RelDataType type, boolean isPrimaryKey, boolean isParentPrimaryKey,
                       boolean notnull, boolean isInternal) {
            fields.add(new Column(name, 0, columnCounter++, type,
                    isPrimaryKey, isParentPrimaryKey,
                    notnull? List.of(NotNull.INSTANCE) : List.of(), isInternal));
        }

        public RelDataType getRowType() {
            List<RelDataTypeField> fields = this.fields.stream()
                    .filter(f->f instanceof Column)
                    .map(f->(Column)f)
                    .map(Column::getRelDataTypeField)
                    .collect(Collectors.toList());
            return new SqrlCalciteTable(fields);
        }

        private void addParentPrimaryKeys(Table parent) {
            for (Column ppk : parent.getPrimaryKeys()) {
                //TODO: append suffix to name to ensure uniqueness on child; rework #createParentChildRelation correspondingly
                addColumn(ppk.getName(), ppk.getDatatype(), true, true, true, true);
            }
        }

        public Table createTable(Table.Type type, Column timestamp, RelNode head, TableStatistic statistic) {
            return new Table(tableIdCounter.incrementAndGet(), namePath, type, fields, timestamp, head, statistic);
        }

    }

}
