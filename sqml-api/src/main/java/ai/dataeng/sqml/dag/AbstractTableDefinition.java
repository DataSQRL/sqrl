package ai.dataeng.sqml.dag;

import java.util.List;

public class AbstractTableDefinition implements TableDefinition {

    private QualifiedTableName name;
    private List<ColumnDeclaration> columns;

    @Override
    public <T extends ColumnDeclaration> List<T> getColumns(Class<T> clazz) {
        return null;
    }

    @Override
    public QualifiedTableName getName() {
        return null;
    }

    @Override
    public TableDefinition getLatest() {
        return null;
    }
}
