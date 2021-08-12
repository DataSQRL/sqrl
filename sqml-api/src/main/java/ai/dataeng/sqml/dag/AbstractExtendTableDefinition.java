package ai.dataeng.sqml.dag;

import java.util.List;

public abstract class AbstractExtendTableDefinition implements TableDefinition {

    private TableDefinition baseTable;

    abstract ColumnDeclaration addedColumn();

    @Override
    public <T extends ColumnDeclaration> List<T> getColumns(Class<T> clazz) {
        List<T> base = baseTable.getColumns(clazz);
        ColumnDeclaration added = addedColumn();
        if (clazz.isInstance(added)) base.add((T)added);
        return base;
    }

    @Override
    public QualifiedTableName getName() {
        return baseTable.getName();
    }

    @Override
    public TableDefinition getLatest() {
        return null;
    }
}
