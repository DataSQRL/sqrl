package ai.dataeng.sqml.dag;

/**
 * Imports a table that is defined in another SQML script.
 *
 * Since imported scripts are parsed first, this table definition references the table definition of the imported
 * table.
 */
public class ImportDefinedTableDefinition extends AbstractImportTableDefinition implements ImportTableDefinition {

    private TableDefinition definedTable;

}
