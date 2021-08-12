package ai.dataeng.sqml.dag;

public interface ImportTableDefinition extends TableDefinition {

    /**
     * Note, that the import name may not be the same as the {@link #getName()}
     * since the import statement can place an imported table into the current dataset if imported fully qualified
     * (e.g. `IMPORT dataset.table`) or if renamed on import (e.g. `IMPORT dataset.table AS newtablename`).
     *
     * @return Qualified name of the table in the dataset from which it was imported
     */
    public QualifiedTableName getImportName();

}
