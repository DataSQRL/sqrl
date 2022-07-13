package ai.datasqrl.plan.local;

import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.io.sources.stats.RelationStats;
import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.schema.SourceTableImportMeta;
import ai.datasqrl.schema.AbstractTable;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.ShadowingContainer;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.TableStatistic;
import ai.datasqrl.schema.TableTimestamp;
import ai.datasqrl.schema.input.FlexibleTableConverter;
import ai.datasqrl.schema.input.RelationType;
import ai.datasqrl.schema.type.ArrayType;
import ai.datasqrl.schema.type.Type;
import ai.datasqrl.schema.type.basic.BasicType;
import ai.datasqrl.schema.type.basic.DateTimeType;
import ai.datasqrl.schema.type.basic.IntegerType;
import ai.datasqrl.schema.type.basic.UuidType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.tuple.Pair;

public class BundleTableFactory {

    public static final AtomicInteger tableIdCounter = new AtomicInteger(0);
    private final Name parentRelationshipName = ReservedName.PARENT;
    private final Map<Name,Integer> defaultTimestampPreference = ImmutableMap.of(
            ReservedName.SOURCE_TIME, 6,
            ReservedName.INGEST_TIME, 3,
            Name.system("timestamp"), 20,
            Name.system("time"), 8);

    public BundleTableFactory() {
    }

    public Pair<Table,Map<Table, SourceTableImportMeta.RowType>> importTable(ImportManager.SourceTableImport impTbl,
                                                                      Optional<Name> tableAlias) {
        ImportVisitor visitor = new ImportVisitor();
        FlexibleTableConverter converter = new FlexibleTableConverter(impTbl.getSchema(), tableAlias);
        converter.apply(visitor);
        TableBuilder tblBuilder = visitor.lastCreatedTable;
        assert tblBuilder != null;
        Map<Table, SourceTableImportMeta.RowType> tables = new HashMap<>();
        Table rootTable = createImportTableHierarchy(tblBuilder, impTbl.getTable().getStatistics(), tables);
        return Pair.of(rootTable,tables);
    }

    private Table createImportTableHierarchy(TableBuilder tblBuilder, SourceTableStatistics statistics,
                                             Map<Table, SourceTableImportMeta.RowType> tables) {
        NamePath tblPath = tblBuilder.getPath();
        RelationStats stats = statistics.getRelationStats(tblPath.subList(1,tblPath.getLength()));
        Table table = tblBuilder.createTable(Table.Type.STREAM, TableStatistic.from(stats));
        tables.put(table,tblBuilder.rowType);
        //Recurse through children and add parent-child relationships
        for (Pair<TableBuilder,Relationship.Multiplicity> child : tblBuilder.children) {
            TableBuilder childBuilder = child.getKey();
            //Add parent timestamp as internal column
            Table childTbl = createImportTableHierarchy(childBuilder, statistics, tables);
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
                    childTable, parentTable, Relationship.JoinType.PARENT, Relationship.Multiplicity.ONE);
            return Optional.of(parentRel);
        }
        return Optional.empty();
    }


    public Relationship createChildRelationship(Name childName, Table childTable, Table parentTable,
                                              Relationship.Multiplicity multiplicity) {
        Relationship childRel = new Relationship(childName,
                parentTable, childTable, Relationship.JoinType.CHILD, multiplicity,
            Optional.empty(), Optional.empty());
        return childRel;
    }

    protected class ImportVisitor implements FlexibleTableConverter.Visitor<Type> {

        private final Deque<TableBuilder> stack = new ArrayDeque<>();
        protected TableBuilder lastCreatedTable = null;


        @Override
        public void beginTable(Name name, NamePath namePath, boolean isNested, boolean isSingleton, boolean hasSourceTimestamp) {
            TableBuilder tblBuilder = new TableBuilder(namePath.concat(name));
            //Add primary keys
            if (isNested) {
                //Add parent primary keys
                tblBuilder.addParentPrimaryKeys(stack.getFirst());
                //Add denormalized timestamp placeholder
                Column timestamp = tblBuilder.addColumn(ReservedName.TIMESTAMP,false,
                        false, DateTimeType.INSTANCE, true, false);
                tblBuilder.setTimestamp(timestamp);
                if (!isSingleton) {
                    tblBuilder.addColumn(ReservedName.ARRAY_IDX,true,
                            false, IntegerType.INSTANCE, true, true);
                }
            } else {
                tblBuilder.addColumn(ReservedName.UUID,true,
                        false, UuidType.INSTANCE, true, true);
                tblBuilder.addColumn(ReservedName.INGEST_TIME, false, false, DateTimeType.INSTANCE, true, true);
                if (hasSourceTimestamp) {
                    tblBuilder.addColumn(ReservedName.SOURCE_TIME, false, false, DateTimeType.INSTANCE, true, true);
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
            TableBuilder tblBuilder = stack.getFirst();
            if (isRelationType(type)) {
                //It's a relationship
                Relationship.Multiplicity multi = Relationship.Multiplicity.ZERO_ONE;
                if (type instanceof ArrayType) multi = Relationship.Multiplicity.MANY;
                else if (notnull) multi = Relationship.Multiplicity.ONE;
                tblBuilder.addChild(lastCreatedTable,multi);
                lastCreatedTable = null;
            } else {
                //It's a column, determine if it is a default timestamp candidate
                Column column = tblBuilder.addColumn(name, false,
                        false, type, notnull, true);
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

        private final SourceTableImportMeta.RowType rowType = new SourceTableImportMeta.RowType();
        private final List<Pair<TableBuilder, Relationship.Multiplicity>> children = new ArrayList<>();
        private Pair<Column, Integer> timestampCandidate = null;

        private TableBuilder(NamePath namePath) {
            super(tableIdCounter.incrementAndGet(), namePath, new ShadowingContainer<>());
        }

        private void addChild(TableBuilder table, Relationship.Multiplicity multi) {
            children.add(Pair.of(table,multi));
        }

        public void setTimestamp(Column column) {
            setTimestampCol(column,Integer.MAX_VALUE);
        }

        private void setTimestampCol(Column column, int preference) {
            timestampCandidate = Pair.of(column,preference);
        }

        public Column addColumn(Name name, boolean isPrimaryKey, boolean isParentPrimaryKey,
                                boolean isVisible) {
            int version = getNextColumnVersion(name);
            Column col = new Column(name, version, getNextColumnIndex(),
                    isPrimaryKey, isParentPrimaryKey, isVisible);
            fields.add(col);
            return col;
        }

        private Column addColumn(Name name, boolean isPrimaryKey, boolean isParentPrimaryKey,
                       Type type, boolean notnull, boolean isVisible) {
            Column column = addColumn(name, isPrimaryKey, isParentPrimaryKey, isVisible);
            rowType.add(column.getIndex(),new SourceTableImportMeta.ColumnType(type,notnull));
            //Check if this is a candidate for timestamp
            if (notnull && (type instanceof DateTimeType) &&
                    defaultTimestampPreference.containsKey(name)) {
                //this column is a candidate for timestamp
                Integer preference = defaultTimestampPreference.get(name);
                if (timestampCandidate == null || timestampCandidate.getValue() < preference) {
                    setTimestampCol(column,preference);
                }
            }
            return column;
        }

        private void addParentPrimaryKeys(TableBuilder parent) {
            for (Column ppk : parent.getPrimaryKeys()) {
                Column copiedPpk = addColumn(ppk.getName(), true, true,
                        false);
                rowType.add(copiedPpk.getIndex(),parent.rowType.get(ppk.getIndex()));
            }
        }

        public Table createTable(Table.Type type, TableStatistic statistic) {
            if (timestampCandidate==null) { //TODO: remove once timestamps are properly propagated
                timestampCandidate = Pair.of(null, Integer.MAX_VALUE);
            }
            Preconditions.checkState(timestampCandidate!=null, "Missing timestamp column");
            TableTimestamp timestamp = TableTimestamp.of(timestampCandidate.getKey(),
                    timestampCandidate.getValue()==Integer.MAX_VALUE? TableTimestamp.Status.INFERRED :
                            TableTimestamp.Status.DEFAULT);
            return new Table(uniqueId, path, type, fields, timestamp, statistic);
        }
    }
}