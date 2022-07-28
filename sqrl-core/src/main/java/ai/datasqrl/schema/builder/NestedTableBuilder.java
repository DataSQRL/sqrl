package ai.datasqrl.schema.builder;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.FieldList;
import ai.datasqrl.schema.Relationship;
import lombok.*;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class NestedTableBuilder<T, X extends NestedTableBuilder<T,X>> implements TableBuilder<T, X> {

    @Getter
    final X parent;
    final FieldList fields = new FieldList();

    public NestedTableBuilder(X parent) {
        this.parent = parent;
    }

    public List<Column<T>> getColumns(boolean onlyVisible, boolean includeNested) {
        Stream<Field> s = fields.getFields(onlyVisible);
        if (includeNested) s = s.map(f -> {
            if (f instanceof ChildRelationship) {
                return ((ChildRelationship<T,X>)f).toColumn();
            } else return f;
        });

        return s.filter(Column.class::isInstance).map(x -> (Column<T>)x)
                .collect(Collectors.toList());
    }

    public List<Field> getAllFields() {
        return fields.getFields(false).collect(Collectors.toList());
    }

    protected void addColumn(Column<T> colum) {
        fields.addField(colum);
    }

    @Override
    public void addColumn(final Name colName, T type, boolean nullable) {
        //A name may clash with a previously added name, hence we increase the version
        int version = fields.nextVersion(colName);
        fields.addField(new Column(colName, version, type, nullable, true));
    }

    @Override
    public void addChild(Name name, X child, T type, Relationship.Multiplicity multiplicity) {
        int version = fields.nextVersion(name);
        fields.addField(new ChildRelationship<>(name,version,child,type,multiplicity));
    }

    @Getter
    public static class Column<T> extends Field {

        final T type;
        final boolean nullable;
        final boolean visible;

        public Column(Name name, int version, T type, boolean nullable, boolean visible) {
            super(name, version);
            this.type = type;
            this.nullable = nullable;
            this.visible = visible;
        }

    }

    @Getter
    public static class ChildRelationship<T,X> extends Field {

        final X childTable;
        final T type;
        final Relationship.Multiplicity multiplicity;

        public ChildRelationship(Name name, int version, X childTable, T type,
                                 Relationship.Multiplicity multiplicity) {
            super(name,version);
            this.childTable = childTable;
            this.type = type;
            this.multiplicity = multiplicity;
        }

        Column<T> toColumn() {
            return new Column<>(name,version,type,multiplicity== Relationship.Multiplicity.ZERO_ONE,isVisible());
        }
    }


}
