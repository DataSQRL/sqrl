package ai.datasqrl.schema.builder;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.plan.calcite.table.TimestampHolder;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.SQRLTable;
import ai.datasqrl.schema.input.SqrlTypeConverter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import lombok.NonNull;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.util.Pair;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

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

    public TimestampHolder.Base getTimestampHolder(UniversalTableBuilder<T> tblBuilder) {
        Preconditions.checkArgument(tblBuilder.getParent()==null,"Can only be invoked on root table");
        TimestampHolder.Base tsh = new TimestampHolder.Base();
        tblBuilder.getAllIndexedFields().filter(f -> f.getField() instanceof NestedTableBuilder.Column)
                .filter(f -> isTimestamp(((NestedTableBuilder.Column<T>)f.getField()).getType()))
                .forEach(f -> tsh.addCandidate(f.getIndex(),getTimestampScore(f.getField().getName())));
        return tsh;
    }


    public interface VirtualTableBuilder<T,V> {

        V make(@NonNull AbstractTableFactory.UniversalTableBuilder<T> tblBuilder);

        V make(@NonNull AbstractTableFactory.UniversalTableBuilder<T> tblBuilder, V parent, Name shredFieldName);
    }

    public Map<SQRLTable,V> build(UniversalTableBuilder<T> builder, VirtualTableBuilder<T,V> vtableBuilder) {
        Map<SQRLTable,V> createdTables = new HashMap<>();
        build(builder,null,null,null,vtableBuilder,createdTables);
        return createdTables;
    }

    private void build(UniversalTableBuilder<T> builder, SQRLTable parent, V vParent,
                       NestedTableBuilder.ChildRelationship<T,UniversalTableBuilder<T>> childRel,
                       VirtualTableBuilder<T,V> vtableBuilder,
                       Map<SQRLTable,V> createdTables) {
        V vTable;
        if (parent==null) vTable = vtableBuilder.make(builder);
        else vTable = vtableBuilder.make(builder,vParent,childRel.getId());
        SQRLTable tbl = new SQRLTable(builder.getPath());
        createdTables.put(tbl,vTable);
        if (parent!=null) {
            //Add child relationship
            createChildRelationship(childRel.getName(), tbl, parent, childRel.getMultiplicity());
        }
        //Add all fields to proxy
        for (Field field : builder.getAllFields()) {
            if (field instanceof NestedTableBuilder.Column) {
                NestedTableBuilder.Column<T> c = (NestedTableBuilder.Column)field;
                tbl.addColumn(c.getName(),c.isVisible(), (BasicSqlType)c.getType());
            } else {
                NestedTableBuilder.ChildRelationship<T,UniversalTableBuilder<T>> child = (NestedTableBuilder.ChildRelationship)field;
                build(child.getChildTable(),tbl,vTable,child,vtableBuilder,createdTables);
            }
        }
        //Add parent relationship if not overwriting column
        if (parent!=null) {
            createParentRelationship(tbl, parent);
        }
    }


    protected<F> UniversalTableBuilder<T> createBuilder(@NonNull NamePath path, UniversalTableBuilder<T> parent,
                                                        T type, int numPrimaryKeys, LinkedHashMap<Integer,Name> index2Name,
                                                        TypeIntrospector<T, F> introspector) {
        UniversalTableBuilder<T> tblBuilder;
        if (parent == null) {
            tblBuilder = new UniversalTableBuilder<>(path.getLast(),path,numPrimaryKeys);
        } else {
            //We assume that the first field is the primary key for the nested table (and don't add an explicit IDX column
            //like we do for imports)
            tblBuilder = new UniversalTableBuilder<>(path.getLast(),path,parent,numPrimaryKeys==0);
        }
        //Add fields
        int index = 0;
        for (F field : introspector.getFields(type)) {
            boolean isVisible = (index2Name==null);
            Name name = introspector.getName(field);
            if (index2Name!=null && index2Name.containsKey(index)) {
                name = index2Name.get(index);
                isVisible = true;
            }
            Optional<Pair<T, Relationship.Multiplicity>> nested = introspector.getNested(field);
            if (nested.isPresent()) {
                Pair<T, Relationship.Multiplicity> rel = nested.get();
                UniversalTableBuilder<T> child = createBuilder(path.concat(name),tblBuilder,
                        rel.getKey(), rel.getValue() == Relationship.Multiplicity.MANY?1:0, null, introspector);
                tblBuilder.addChild(name,child,rel.getKey(),rel.getValue());
            } else {
                tblBuilder.addColumn(name, introspector.getType(field), introspector.isNullable(field), isVisible);
            }
            index++;
        }
        return tblBuilder;
    }

    public interface TypeIntrospector<Type,Field> {

        Iterable<Field> getFields(Type type);

        Name getName(Field field);

        Type getType(Field field);

        boolean isNullable(Field field);

        Optional<Pair<Type, Relationship.Multiplicity>> getNested(Field field);

        SqrlTypeConverter<Type> getTypeConverter();

    }

}
