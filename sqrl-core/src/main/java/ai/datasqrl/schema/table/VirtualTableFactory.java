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

public abstract class VirtualTableFactory<T,V extends VirtualTable> extends TableFactory {

    private final AtomicInteger tableIdCounter = new AtomicInteger(0);
    public final Name parentRelationshipName = ReservedName.PARENT;
    private final Map<Name,Integer> defaultTimestampPreference = ImmutableMap.of(
            ReservedName.SOURCE_TIME, 6,
            ReservedName.INGEST_TIME, 3,
            Name.system("timestamp"), 20,
            Name.system("time"), 8);

    private final Map<String,V> tables = new HashMap<>();
    private final List<TableProxy<V>> internalTables = new ArrayList<>();

    public Optional<V> getTable(String tableId) {
        return Optional.ofNullable(tables.get(tableId));
    }

    public ImportBuilderFactory<T> getImportFactory() {
        return new ImportBuilderFactory();
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




    public interface VirtualTableBuilder<T,V> {

        V make(@NonNull TableBuilder<T> tblBuilder);

        V make(@NonNull TableBuilder<T> tblBuilder, V parent, Name shredFieldName);
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



    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        for (TableProxy t : internalTables) {
            s.append(t).append("\n");
        }
        return s.toString();
    }
}
