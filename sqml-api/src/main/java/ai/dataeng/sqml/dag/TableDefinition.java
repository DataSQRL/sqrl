package ai.dataeng.sqml.dag;

/**
 * A {@link TableDefinition} is an explicitly defined relation within an SQML script.
 * It can be uniquely identified by name and can be incrementally defined or extended.
 *
 * Tables can be nested within other tables which means that tables names are paths and
 * that implicit reference fields (see {@link ImplicitReferenceColumnDeclaration}) are
 * automatically introduced. However, outside the naming convention and automatic definition
 * of certain reference fields, nested tables are normal relations.
 *
 */
public interface TableDefinition extends RelationDefinition {

    /**
     *
     *
     * @return The full table name
     */
    public QualifiedTableName getName();

    /**
     * Since tables can be defined incrementally, this method returns
     * the most recent definition of the table.
     *
     * @return The latest {@link TableDefinition} for this table
     */
    public TableDefinition getLatest();

}
