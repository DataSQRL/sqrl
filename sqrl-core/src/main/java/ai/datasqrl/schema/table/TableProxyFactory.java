package ai.datasqrl.schema.table;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.schema.table.builder.NestedTableBuilder;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.util.Pair;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public abstract class TableProxyFactory<T,V extends VirtualTable> {

    private final AtomicInteger tableIdCounter = new AtomicInteger(0);
    public final Name parentRelationshipName = ReservedName.PARENT;
    private final Map<Name,Integer> defaultTimestampPreference = ImmutableMap.of(
            ReservedName.SOURCE_TIME, 6,
            ReservedName.INGEST_TIME, 3,
            Name.system("timestamp"), 20,
            Name.system("time"), 8);

    private final Map<Name,TableProxy<V>> visibleTables = new HashMap<>();
    private final List<TableProxy<V>> internalTables = new ArrayList<>();

    public Optional<TableProxy<V>> walkTable(@NonNull NamePath tablePath) {
        Preconditions.checkArgument(tablePath.getLength()>0);
        Name rootTableName = tablePath.getFirst();
        return Optional.ofNullable(visibleTables.get(rootTableName)).flatMap(t -> t.walkTable(tablePath.popFirst()));
    }

    public Optional<TableProxy> getTable(NamePath name) {
        return Optional.ofNullable(visibleTables.get(name));
    }

    private Name getTableId(Name name) {
        return name.suffix(Integer.toString(tableIdCounter.incrementAndGet()));
    }

    protected int getTimestampScore(Name columnName) {
        return Optional.ofNullable(defaultTimestampPreference.get(columnName)).orElse(1);
    }

    protected abstract boolean isTimestamp(T datatype);

    public Map<NestedTableBuilder.Column<T>, Integer> getTimestampCandidateScores(TableBuilder<T> tblBuilder) {
        Preconditions.checkArgument(tblBuilder.getParent()==null,"Can only be invoked on root table");
        return tblBuilder.getColumns(false,false).stream()
                .filter(c -> isTimestamp(c.getType()))
                .map(c -> Pair.of(c,getTimestampScore(c.getName())))
                .collect(Collectors.toMap(Pair::getKey,Pair::getValue));
    }

    @Getter
    public static class TableBuilder<T> extends NestedTableBuilder<T, TableBuilder<T>> {

        @NonNull
        final NamePath path;
        @NonNull
        final Name id;
        //The first n columns are form the primary key for this table
        // We make the assumption that primary key columns are always first!
        final int numPrimaryKeys;

        public TableBuilder(@NonNull Name id, @NonNull NamePath path, int numPrimaryKeys) {
            super(null);
            this.numPrimaryKeys = numPrimaryKeys;
            this.id = id;
            this.path = path;
        }

        public TableBuilder(@NonNull Name id, @NonNull NamePath path, TableBuilder<T> parent, boolean isSingleton) {
            super(parent);
            //Add parent primary key columns
            Iterator<Column<T>> parentCols = parent.getColumns(false,false).iterator();
            for (int i = 0; i < parent.numPrimaryKeys; i++) {
                Column<T> ppk = parentCols.next();
                addColumn(new Column<>(ppk.name,ppk.version,ppk.getType(),ppk.isNullable(),false));
            }
            this.numPrimaryKeys = parent.numPrimaryKeys + (isSingleton?0:1);
            this.path = path;
            this.id = id;
        }

    }

    public List<TableProxy<V>> build(TableBuilder<T> builder, VirtualTableBuilder<T,V> vtableBuilder) {
        List<TableProxy<V>> createdTables = new ArrayList<>();
        build(builder,null,null,vtableBuilder,createdTables);
        TableProxy<V> rootTable = createdTables.get(0);
        internalTables.addAll(createdTables);
        if (builder.getPath().getLength()==1) { //It's a root table that's visible
            visibleTables.put(builder.getPath().getFirst(), rootTable);
        }
        return createdTables;
    }

    public interface VirtualTableBuilder<T,V> {

        V make(@NonNull TableBuilder<T> tblBuilder);

        V make(@NonNull TableBuilder<T> tblBuilder, V parent, Name shredFieldName);
    }

    private void build(TableBuilder<T> builder, TableProxy<V> parent,
                       NestedTableBuilder.ChildRelationship<T,TableBuilder<T>> childRel,
                       VirtualTableBuilder<T,V> vtableBuilder,
                       List<TableProxy<V>> createdTables) {
        V vTable;
        if (parent==null) vTable = vtableBuilder.make(builder);
        else vTable = vtableBuilder.make(builder,parent.getVirtualTable(),childRel.getId());
        TableProxy<V> tbl = new TableProxy(builder.getId(), builder.getPath(), vTable);
        createdTables.add(tbl);
        if (parent!=null) {
            //Add child relationship
            parent.addRelationship(childRel.getName(), tbl,
                    childRel.getMultiplicity(), Relationship.JoinType.CHILD);
        }
        //Add all fields to proxy
        for (Field field : builder.getAllFields()) {
            if (field instanceof NestedTableBuilder.Column) {
                NestedTableBuilder.Column<T> c = (NestedTableBuilder.Column)field;
                tbl.addColumnReference(c.getName(),c.isVisible());
            } else {
                NestedTableBuilder.ChildRelationship<T,TableBuilder<T>> child = (NestedTableBuilder.ChildRelationship)field;
                build(child.getChildTable(),tbl,child,vtableBuilder,createdTables);
            }
        }
        //Add parent relationship if not overwriting column
        if (builder.getParent()!=null && tbl.getField(parentRelationshipName).isEmpty()) {
            tbl.addRelationship(parentRelationshipName, parent,
                            Relationship.Multiplicity.ONE, Relationship.JoinType.PARENT);
        }
    }

    public final class ImportBuilderFactory<T> implements ai.datasqrl.schema.table.builder.TableBuilder.Factory<T, TableProxyFactory.TableBuilder<T>> {

        @Override
        public TableBuilder<T> createTable(@NonNull Name name, @NonNull NamePath path, @NonNull TableBuilder<T> parent, boolean isSingleton) {
            return new TableBuilder(getTableId(name), path.concat(name), parent, isSingleton);
        }

        @Override
        public TableBuilder<T> createTable(@NonNull Name name, @NonNull NamePath path) {
            return new TableBuilder(getTableId(name), path.concat(name), 1);
        }
    }

    public ImportBuilderFactory<T> getImportFactory() {
        return new ImportBuilderFactory();
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        for (TableProxy t : internalTables) {
            s.append(t).append("\n");
        }
        return s.toString();
    }
}
