package ai.datasqrl.schema.builder;

import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.input.FlexibleTableConverter;
import ai.datasqrl.schema.input.RelationType;
import ai.datasqrl.schema.input.TableBuilderFlexibleTableConverterVisitor;
import ai.datasqrl.schema.type.ArrayType;
import ai.datasqrl.schema.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class TableFactory extends AbstractTableFactory {

    public final Name parentRelationshipName = ReservedName.PARENT;

    public List<Table> importTables(ImportManager.SourceTableImport importSource, Optional<Name> nameAlias) {
        ImportSchemaVisitor importVisitor = getImportVisitor();
        new FlexibleTableConverter(importSource.getSchema(), nameAlias).apply(importVisitor);
        AbstractTableFactory.UniversalTableBuilder<Type> tableBuilder = importVisitor.getRootTable();
        return build(tableBuilder);
    }

    /**
     * Builds all (nested) SQRL schema tables from the provided
     * {@link ai.datasqrl.schema.builder.AbstractTableFactory.UniversalTableBuilder}.
     * and the parent-child relationships between them.
     *
     * The first table in the returned list is always the root table and
     * any subsequent tables are child tables.
     *
     * @param builder
     * @param <T> type parameter for the column data type - ignored by this method
     * @return List of SQRL schema tables that are built
     */
    public<T> List<Table> build(UniversalTableBuilder<T> builder) {
        List<Table> createdTables = new ArrayList<>();
        build(builder,null,null,createdTables);
        return createdTables;
    }

    private<T> void build(UniversalTableBuilder<T> builder, Table parent,
                          NestedTableBuilder.ChildRelationship<T, UniversalTableBuilder<T>> childRel,
                          List<Table> createdTables) {
        Table tbl = new Table(builder.getPath());
        createdTables.add(tbl);
        if (parent!=null) {
            //Add child relationship
            createChildRelationship(childRel.getName(), tbl, parent, childRel.getMultiplicity());

        }
        //Add all fields to proxy
        for (Field field : builder.getAllFields()) {
            if (field instanceof NestedTableBuilder.Column) {
                NestedTableBuilder.Column c = (NestedTableBuilder.Column)field;
                tbl.addColumn(c.getName(),c.isVisible());
            } else {
                NestedTableBuilder.ChildRelationship<T, UniversalTableBuilder<T>> child = (NestedTableBuilder.ChildRelationship)field;
                build(child.getChildTable(),tbl,child,createdTables);
            }
        }
        //Add parent relationship if not overwriting column
        if (parent!=null && tbl.getField(parentRelationshipName).isEmpty()) {
            createParentRelationship(tbl, parent);
        }
    }

    public Optional<Relationship> createParentRelationship(Table childTable, Table parentTable) {
        //Avoid overwriting an existing "parent" column on the child
        if (childTable.getField(parentRelationshipName).isEmpty()) {
            return Optional.of(childTable.addRelationship(parentRelationshipName, parentTable, Relationship.JoinType.PARENT,
                    Relationship.Multiplicity.ONE));
        }
        return Optional.empty();
    }


    public Relationship createChildRelationship(Name childName, Table childTable, Table parentTable,
                                                Relationship.Multiplicity multiplicity) {
        return parentTable.addRelationship(childName, childTable,
                Relationship.JoinType.CHILD, multiplicity);
    }

    protected ImportSchemaVisitor getImportVisitor() {
        return new ImportSchemaVisitor();
    }

    /**
     * A no-op visitor to get the {@link ai.datasqrl.schema.builder.AbstractTableFactory.UniversalTableBuilder} for
     * an imported table to then build the SQRL schema tables.
     */
    private class ImportSchemaVisitor extends TableBuilderFlexibleTableConverterVisitor<Type, UniversalTableBuilder<Type>> {

        protected ImportSchemaVisitor() {
            super(new ImportBuilderFactory<>());
        }

        @Override
        public Type wrapArray(Type type, boolean nullable) {
            return new ArrayType(type);
        }

        @Override
        protected Optional<Type> createTable(UniversalTableBuilder<Type> tblBuilder) {
            return Optional.of(RelationType.EMPTY);
        }

        @Override
        public Type nullable(Type type, boolean nullable) {
            return type;
        }

        @Override
        protected SqrlTypeConverter<Type> getTypeConverter() {
            return new SqrlTypeConverter<Type>() {
                @Override
                public Type visitType(Type type, Void context) {
                    return type;
                }
            };
        }
    }


}
