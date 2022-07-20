package ai.datasqrl.schema.builder;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.schema.Field;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import lombok.NonNull;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class VirtualTableFactory<T,V extends VirtualTable> extends AbstractTableFactory {

    private final Map<Name,Integer> defaultTimestampPreference = ImmutableMap.of(
            ReservedName.SOURCE_TIME, 6,
            ReservedName.INGEST_TIME, 3,
            Name.system("timestamp"), 20,
            Name.system("time"), 8);

    public ImportBuilderFactory<T> getImportFactory() {
        return new ImportBuilderFactory();
    }

    protected int getTimestampScore(Name columnName) {
        return Optional.ofNullable(defaultTimestampPreference.get(columnName)).orElse(1);
    }

    protected abstract boolean isTimestamp(T datatype);

    public Map<NestedTableBuilder.Column<T>, Integer> getTimestampCandidateScores(UniversalTableBuilder<T> tblBuilder) {
        Preconditions.checkArgument(tblBuilder.getParent()==null,"Can only be invoked on root table");
        return tblBuilder.getColumns(false,false).stream()
                .filter(c -> isTimestamp(c.getType()))
                .map(c -> Pair.of(c,getTimestampScore(c.getName())))
                .collect(Collectors.toMap(Pair::getKey,Pair::getValue));
    }




    public interface VirtualTableBuilder<T,V> {

        V make(@NonNull AbstractTableFactory.UniversalTableBuilder<T> tblBuilder);

        V make(@NonNull AbstractTableFactory.UniversalTableBuilder<T> tblBuilder, V parent, Name shredFieldName);
    }

    public List<V> build(UniversalTableBuilder<T> builder, VirtualTableBuilder<T,V> vtableBuilder) {
        List<V> createdTables = new ArrayList<>();
        build(builder,null,null,vtableBuilder,createdTables);
        return createdTables;
    }

    private void build(UniversalTableBuilder<T> builder, V parent,
                       NestedTableBuilder.ChildRelationship<T, UniversalTableBuilder<T>> childRel,
                       VirtualTableBuilder<T,V> vtableBuilder,
                       List<V> createdTables) {
        V vTable;
        if (parent==null) vTable = vtableBuilder.make(builder);
        else vTable = vtableBuilder.make(builder,parent,childRel.getId());
        createdTables.add(vTable);
        //Add all fields to proxy
        for (Field field : builder.getAllFields()) {
            if (field instanceof NestedTableBuilder.ChildRelationship) {
                NestedTableBuilder.ChildRelationship<T, UniversalTableBuilder<T>> child = (NestedTableBuilder.ChildRelationship)field;
                build(child.getChildTable(),vTable,child,vtableBuilder,createdTables);
            }
        }
    }

}
