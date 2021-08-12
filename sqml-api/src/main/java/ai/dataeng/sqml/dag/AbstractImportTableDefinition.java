package ai.dataeng.sqml.dag;

public class AbstractImportTableDefinition extends AbstractTableDefinition implements ImportTableDefinition {

    private QualifiedTableName importName;

    @Override
    public QualifiedTableName getImportName() {
        return importName;
    }
}
