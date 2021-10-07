package ai.dataeng.sqml.dag;


import ai.dataeng.sqml.schema2.Type;

/**
 * Defines a "standard" column in the sense of relational algebra in that it contains data of a particular type.
 */
public class DataColumnDeclaration extends ColumnDeclaration {

    //TODO: This can only be a scalar or array type
    private Type type;
    private boolean notNull;

    public DataColumnDeclaration(String name, Type type, boolean notNull) {
        super(name);
        this.type = type;
        this.notNull = notNull;
    }

}
