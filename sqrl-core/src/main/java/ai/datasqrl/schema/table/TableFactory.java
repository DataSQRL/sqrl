package ai.datasqrl.schema.table;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.table.builder.NestedTableBuilder;
import lombok.Getter;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TableFactory {

    public<T> List<Table> build(TableBuilder<?> builder) {
        List<Table> createdTables = new ArrayList<>();
        build(builder,null,null,vtableBuilder,createdTables);
        TableProxy<V> rootTable = createdTables.get(0);
        internalTables.addAll(createdTables);
        if (builder.getPath().getLength()==1) { //It's a root table that's visible
            visibleTables.put(builder.getPath().getFirst(), rootTable);
        }
        return createdTables;
    }

    private void build(TableBuilder<?> builder, Table parent,
                       NestedTableBuilder.ChildRelationship<?,TableBuilder<?>> childRel,
                       List<Table> createdTables) {
        TableProxy<V> tbl = new Table(builder.getId(), builder.getPath());
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

    public final class ImportBuilderFactory<T> implements TableBuilder.Factory<T, VirtualTableFactory.TableBuilder<T>> {

        @Override
        public TableBuilder<T> createTable(@NonNull Name name, @NonNull NamePath path, @NonNull TableBuilder<T> parent, boolean isSingleton) {
            return new TableBuilder(name, path.concat(name), parent, isSingleton);
        }

        @Override
        public TableBuilder<T> createTable(@NonNull Name name, @NonNull NamePath path) {
            return new TableBuilder(name, path.concat(name), 1);
        }
    }

}
