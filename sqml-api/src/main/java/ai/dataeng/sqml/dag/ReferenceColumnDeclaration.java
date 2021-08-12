package ai.dataeng.sqml.dag;

/**
 * A reference column is a special type of column that declares a JOIN to another table and therefore references
 * rows in that table. By accessing the column the JOIN is instantiated in a particular context.
 */
public abstract class ReferenceColumnDeclaration extends ColumnDeclaration {

    private RelationDefinition referencedTable;
    private Cardinality cardinality;

    public ReferenceColumnDeclaration(String name, RelationDefinition referencedTable, Cardinality cardinality) {
        super(name);
        this.referencedTable = referencedTable;
        this.cardinality = cardinality;
    }

    public enum Cardinality {
        SINGLE, MULTIPLE;
    }

}
