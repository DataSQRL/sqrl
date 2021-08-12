package ai.dataeng.sqml.dag;

import ai.dataeng.sqml.tree.Expression;

/**
 * A table that is defined by adding a data column defined by an expression
 */
public class ExtendDataColumnTableDefinition extends AbstractExtendTableDefinition {

    private DataColumnDeclaration column;
    private Expression columnExpression;
    private Scope scope;

    @Override
    ColumnDeclaration addedColumn() {
        return column;
    }
}
